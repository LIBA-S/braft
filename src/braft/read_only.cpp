/*
 *Copyright (c) 2023, Tencent. All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of elasticfaiss nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "braft/read_only.h"

#include "braft/node.h"

namespace braft {

void RemoteReadIndexClosure::Run() {
    brpc::ClosureGuard done_guard(_done);
    if (!status().ok()) {
        _cntl->SetFailed(status().error_code(), "%s", status().error_cstr());
        return;
    }
    _response->set_success(status().ok());
    _response->set_read_index(index());
    delete this;
}

WaitReadIndexClosure::WaitReadIndexClosure(const bool need_apply, LinearizableClosure* done, NodeImpl* node)
    : _need_apply(need_apply)
    , _done(done)
    , _node(node) {
    if (_node) {
        _node->AddRef();
    }
}

WaitReadIndexClosure::~WaitReadIndexClosure() {
    if (_node) {
        _node->Release();
    }
}

void WaitReadIndexClosure::Run() {
    if (!status().ok()) {
        _done->status() = status();
        run_closure_in_bthread(_done);
    } else if (_need_apply) {
        // Type Check
        LocalReadIndexClosure* closure = static_cast<LocalReadIndexClosure*>(_done);
        _node->wait_on_apply(closure);
    }
    delete this;
}

void ReadOnly::add_request(Ballot ballot, WaitReadIndexClosure* closure) {
    if (_pending_read_index.find(closure->unique_id()) != _pending_read_index.end()) {
        return;
    }

    BallotAndClosure bac;
    bac.ballot = ballot;
    bac.done = closure;

    _pending_read_index[closure->unique_id()] = bac;
    _read_index_queue.push_back(closure->unique_id());
}

bool ReadOnly::recv_ack(const PeerId& peer_id, const std::string& unique_id) {
    BallotAndClosureMap::iterator iter = _pending_read_index.find(unique_id);
    if (iter == _pending_read_index.end()) {
        return false;
    }
    iter->second.ballot.grant(peer_id);
    return iter->second.ballot.granted();
}

ReadOnly::BallotAndClosureVec ReadOnly::advance(const std::string& unique_id) {
    BallotAndClosureVec ready_read_index;

    bool found = false;
    UniqueIDQueue::iterator ri_iter = _read_index_queue.begin();
    for (; ri_iter != _read_index_queue.end() && !found; ++ri_iter) {
        BallotAndClosureMap::iterator rc_iter = _pending_read_index.find(unique_id);
        if (rc_iter == _pending_read_index.end()) {
            LOG(FATAL) << "cannot find corresponding read state from pending map";
        }
        ready_read_index.push_back(rc_iter->second);
        found = *ri_iter == unique_id;
    }

    if (found) {
        _read_index_queue.erase(_read_index_queue.begin(), ri_iter);
        for (BallotAndClosureVec::const_iterator iter = ready_read_index.begin();
                iter != ready_read_index.end(); ++iter) {
            _pending_read_index.erase(iter->done->unique_id());
        }
    }

    return ready_read_index;
}

std::string ReadOnly::last_pending_unique_id() const {
    if (_read_index_queue.empty()) {
        return "";
    }
    return _read_index_queue.back();
}

}  // namespace braft
