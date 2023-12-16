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

#ifndef BRAFT_READ_ONLY_H
#define BRAFT_READ_ONLY_H

#include <list>
#include <string>
#include <unordered_map>

#include <bthread/countdown_event.h>
#include <brpc/controller.h>
#include <butil/time.h>
#include <butil/endpoint.h>

#include "braft/ballot.h"
#include "braft/macros.h"
#include "braft/raft.h"
#include "braft/raft.pb.h"
#include "braft/util.h"

namespace braft {

class NodeImpl;

class LinearizableClosure : public Closure {
public:
    virtual void Run() = 0;

    int64_t index() const { return _index; }
    void set_index(const int64_t index) { _index = index; }

    const std::string& unique_id() const { return _unique_id; }
    void set_unique_id(const std::string& unique_id) { _unique_id = unique_id; }

protected:
    LinearizableClosure()
        : _index(-1) {}
    LinearizableClosure(const int64_t index, const std::string& unique_id)
        : _index(index)
        , _unique_id(unique_id) {}

private:
    int64_t _index;
    std::string _unique_id;
};

class RemoteReadIndexClosure : public LinearizableClosure {
public:
    void Run();

    RemoteReadIndexClosure(brpc::Controller* cntl,
                           const ReadIndexRequest* request,
                           ReadIndexResponse* response,
                           google::protobuf::Closure* done)
        : _cntl(cntl)
        , _request(request)
        , _response(response)
        , _done(done) {}

    ~RemoteReadIndexClosure() {}

private:
    brpc::Controller* _cntl;
    const ReadIndexRequest* _request;
    ReadIndexResponse* _response;
    google::protobuf::Closure* _done;
};

class LocalReadIndexClosure : public LinearizableClosure {
public:
    LocalReadIndexClosure() : _event(1) {}
    ~LocalReadIndexClosure() = default;

    void Run() { _event.signal(); }
    void wait() { _event.wait(); }

private:
    bthread::CountdownEvent _event;
};

class WaitReadIndexClosure : public Closure {
public:
    WaitReadIndexClosure(const bool need_apply, LinearizableClosure* done, NodeImpl* node);
    ~WaitReadIndexClosure();

    void Run();

    int64_t read_index() const { return _done->index(); }
    const std::string& unique_id() const { return _done->unique_id(); }

private:
    const bool _need_apply;
    LinearizableClosure* _done;
    NodeImpl* _node;
};

class RequestIDGenerator {
public:
    static const int kCountLen = 24;

    RequestIDGenerator()
        : _suffix(0) {}

    RequestIDGenerator(const PeerId& peer_id)
        : _suffix(0)
        {
            init(peer_id);
        }

    void init(const PeerId& peer_id) {
        _prefix = std::string(butil::endpoint2str(peer_id.addr).c_str());
        _suffix = butil::cpuwide_time_ms() << kCountLen;
    }

    std::string next() {
        return _prefix + std::to_string(++_suffix);
    }

private:
    std::string _prefix;
    uint64_t _suffix;
};

class ReadOnly {
public:
    struct BallotAndClosure {
        BallotAndClosure() : done(NULL) {}

        Ballot ballot;
        WaitReadIndexClosure* done;
    };

    typedef std::unordered_map<std::string, BallotAndClosure> BallotAndClosureMap;
    typedef std::vector<BallotAndClosure> BallotAndClosureVec;
    typedef std::list<std::string> UniqueIDQueue;

    ReadOnly() = default;

    // Clear all the pending read requests
    void reset();

    // Add a read only request into ReadOnly struct.
    int add_request(Ballot ballot, WaitReadIndexClosure* done);

    // Notify the ReadOnly struct that the raft state machine received
    // an acknowledgment of the heartbeat that attached with the unique id.
    bool recv_ack(const PeerId& peer_id, const std::string& unique_id);

    // Advance the read only request queue kept by the readonly struct.
    // It dequeues the requests until it finds the read only request that has
    // the same unique id.
    BallotAndClosureVec advance(const std::string& unique_id);

    // Fetch the unique id of the last pending read only request in readonly struct.
    std::string last_pending_unique_id();

private:
    raft_mutex_t _mutex;
    BallotAndClosureMap _pending_read_index;
    UniqueIDQueue _read_index_queue;
};


}  //  namespace braft

#endif //~BRAFT_READ_ONLY_H
