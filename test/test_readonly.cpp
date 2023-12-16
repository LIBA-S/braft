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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <fcntl.h>

#include <butil/logging.h>
#include <butil/files/file_path.h>
#include <butil/file_util.h>
#include <butil/fast_rand.h>
#include <brpc/controller.h>
#include <brpc/closure_guard.h>
#include <bthread/bthread.h>
#include <bthread/countdown_event.h>

#include "../test/util.h"
#include "braft/read_only.h"
#include "braft/raft.pb.h"

const static char* kRemoteRead = "RemoteRead";
const static char* kLocalRead = "LocalRead";
const static char* kMixRead = "MixRead";

class TestEnvironment : public ::testing::Environment {
public:
    void SetUp() {
    }
    void TearDown() {
    }
};

struct OnRemoteReadDone : public google::protobuf::Closure {
    OnRemoteReadDone(braft::Closure* done_)
        : done(done_) {}

    virtual ~OnRemoteReadDone() = default;

    void Run() {
        done->status().set_error(cntl.ErrorCode(), cntl.ErrorText());
        run_closure_in_bthread(done);
        delete this;
    }

    braft::ReadIndexRequest request;
    braft::ReadIndexResponse response;
    brpc::Controller cntl;
    braft::Closure* done;
};

class ReadOnlyTest : public testing::TestWithParam<const char*> {
protected:
    void SetUp() {
        GFLAGS_NS::SetCommandLineOption("crash_on_fatal_log", "true");
        if (GetParam() == kRemoteRead) {
            read_mode_ = kRemoteRead;
        } else if (GetParam() == kLocalRead) {
            read_mode_ = kLocalRead;
        } else {
            read_mode_ = kMixRead;
        }
        LOG(INFO) << "Start unitests: " << read_mode_;
    }

    void TearDown() {
        LOG(INFO) << "Stop unitests: " << read_mode_;
    }

private:
    struct LocalClosure {
        LocalClosure() : user_done(nullptr), done(nullptr) {}
        void Run() {
            user_done->Run();

            done->wait();
            delete done;
        }

        braft::Closure* user_done;
        braft::LocalReadIndexClosure* done;
    };
    braft::WaitReadIndexClosure* LinearizableRead(const std::string& request_id, braft::Closure* done) {
        braft::LinearizableClosure* closure = nullptr;
        if (read_mode_ == kMixRead) {
            read_mode_ = rand() % 1 ? kRemoteRead : kLocalRead;
        }
        if (read_mode_ == kRemoteRead) {
            OnRemoteReadDone* remote_read_done = new OnRemoteReadDone(done);
            closure = new braft::RemoteReadIndexClosure(&remote_read_done->cntl,
                                                        &remote_read_done->request,
                                                        &remote_read_done->response,
                                                        remote_read_done->done);
        } else {
            closure = new braft::LocalReadIndexClosure();
            LocalClosure local_closure;
            local_closure.user_done = done;
            local_closure.done = static_cast<braft::LocalReadIndexClosure*>(closure);
            local_closures_.push_back(local_closure);
        }
        closure->set_unique_id(request_id);
        braft::WaitReadIndexClosure* wait_read_closure = new WaitReadIndexClosure(false, closure, nullptr);
        return wait_read_closure;
    }

    ReadOnly read_only_;
    std::string read_mode_;
    std::vector<LocalClosure> local_closures_;
    butil::ShadowingAtExitManager exit_manager_;
};

TEST_P(ReadOnlyTest, base) {
    constexpr static int kRequestNum = 10000;

    braft::PeerId peer_id("1.1.1.1:1000:0");
    braft::Configuration conf;
    conf.add_peer(peer_id);
    braft::Ballot ballot;
    ASSERT_EQ(0, ballot.init(conf, nullptr));

    bthread::CountdownEvent cond(kRequestNum);
    for (int i = 0; i < kRequestNum; ++i) {
        braft::WaitReadIndexClosure* done = LinearizableRead(std::to_string(i), NEW_APPLYCLOSURE(&cond, 0));
        ASSERT_EQ(0, read_only_.add_request(ballot, done));
        ASSERT_TRUE(read_only_.recv_ack(peer_id, std::to_string(i)));
    }
    ASSERT_EQ(read_only_.last_pending_unique_id(), std::to_string(kRequestNum-1));
    braft::ReadOnly::BallotAndClosureVec closure_vec = read_only_.advance(std::to_string(kRequestNum - 1));
    ASSERT_EQ(closure_vec.size(), kRequestNum);
    for (size_t i = 0; i < closure_vec.size(); ++i) {
        closure_vec[i].done->Run();
    }
    for (size_t i = 0; i < local_closures_.size(); ++i) {
        local_closures_[i].Run();
        local_closures_[i];
    }
    cond.wait();
    ASSERT_EQ(read_only_.last_pending_unique_id(), "");
}

TEST_P(ReadOnlyTest, multi_peers) {
    constexpr static int kRequestNum = 10000;

    braft::PeerId peer_id_1("1.1.1.1:1000:0");
    braft::PeerId peer_id_2("1.1.1.1:1001:0");
    braft::PeerId peer_id_3("1.1.1.1:1002:0");
    braft::Configuration conf;
    conf.add_peer(peer_id_1);
    conf.add_peer(peer_id_2);
    conf.add_peer(peer_id_3);
    braft::Ballot ballot;
    ASSERT_EQ(0, ballot.init(conf, nullptr));

    bthread::CountdownEvent cond(kRequestNum);
    for (int i = 0; i < kRequestNum; ++i) {
        braft::WaitReadIndexClosure* done = LinearizableRead(std::to_string(i), NEW_APPLYCLOSURE(&cond, 0));
        ASSERT_EQ(0, read_only_.add_request(ballot, done));
    }
    ASSERT_EQ(read_only_.last_pending_unique_id(), std::to_string(kRequestNum-1));

    int random_id = rand()%(kRequestNum-1);
    const std::string random_unique_id = std::to_string(random_id);
    ASSERT_FALSE(read_only_.recv_ack(peer_id_1, random_unique_id));
    ASSERT_TRUE(read_only_.recv_ack(peer_id_2, random_unique_id));

    braft::ReadOnly::BallotAndClosureVec closure_vec = read_only_.advance(random_unique_id);
    ASSERT_EQ(closure_vec.size(), random_id + 1);
    for (size_t i = 0; i < closure_vec.size(); ++i) {
        closure_vec[i].done->Run();
    }

    int max_id = kRequestNum - 1;
    const std::string max_unique_id = std::to_string(max_id);
    ASSERT_FALSE(read_only_.recv_ack(peer_id_1, max_unique_id));
    ASSERT_TRUE(read_only_.recv_ack(peer_id_3, max_unique_id));

    closure_vec = read_only_.advance(max_unique_id);
    ASSERT_EQ(closure_vec.size(), max_id - random_id);
    for (size_t i = 0; i < closure_vec.size(); ++i) {
        closure_vec[i].done->Run();
    }

    for (size_t i = 0; i < local_closures_.size(); ++i) {
        local_closures_[i].Run();
        local_closures_[i];
    }
    cond.wait();
    ASSERT_EQ(read_only_.last_pending_unique_id(), "");
}

INSTANTIATE_TEST_CASE_P(ReadOnlyTestFromRemote,
                        ReadOnlyTest,
                        ::testing::Values("RemoteRead"));

INSTANTIATE_TEST_CASE_P(ReadOnlyTestFromLocal,
                        ReadOnlyTest,
                        ::testing::Values("LocalRead"));

INSTANTIATE_TEST_CASE_P(ReadOnlyTestFromRemoteAndLocal,
                        ReadOnlyTest,
                        ::testing::Values("MixRead"));

const static int kCheckQuorum = 0;
const static int kLeaseBased = 1;
const static char* kLeaderRead = "LeaderRead";
const static char* kFollowerRead = "FollowerRead";

class DelayApplyFSM : public MockFSM {
protected:
    DelayApplyFSM(uint64_t max_delay_us, const butil::EndPoint& address_)
        : MockFSM(address_), _max_delay_us(max_delay_us) {
        srand((unsigned)time(NULL));
    }

    virtual void on_apply(braft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            ::brpc::ClosureGuard guard(iter.done());
            lock();
            logs.push_back(iter.data());
            unlock();
            applied_index = iter.index();
        }
        //bthread_usleep(rand()%_max_delay_us);
    }

private:
    int i = 0;
    uint64_t _max_delay_us;
};

class LinearizableReadTest : public testing::TestWithParam<std::tuple<int, int, const char*>> {
protected:
    void SetUp() {
        GFLAGS_NS::SetCommandLineOption("crash_on_fatal_log", "true");
        read_type_ = std::get<0>(GetParam());
        multi_peer_ = std::get<1>(GetParam());
        read_role_ = std::get<2>(GetParam());
        ::system("rm -rf data");
        LOG(WARNING) << "Start unitests, check quorum ? " << (read_type_ == 0)
                     << ", multi peer ? " << multi_peer_ << ", role: " << read_role_;
    }

    void TearDown() {
        ::system("rm -rf data");
        LOG(WARNING) << "Stop unitests";
    }

private:
    int read_type_;
    int multi_peer_;
    std::string read_role_;
    butil::ShadowingAtExitManager exit_manager_;
};

struct ApplyArg {
    Cluster* c;
    volatile bool stop;
};

static void* apply_routine(void* arg) {
    ApplyArg* aa = (ApplyArg*)arg;
    while (!aa->stop) {
        aa->c->wait_leader();
        braft::Node* leader = aa->c->leader();
        if (!leader) {
            continue;
        }

        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", rand()%1000);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        leader->apply(task);
        bthread_usleep(rand()%1000);
    }
    return NULL;
}

struct ReadArg {
    Cluster* c;
    volatile bool stop;
    int read_type;
    std::string read_role;
};

static void* read_routine(void* arg) {
    int64_t read_cnt = 0;

    ReadArg* ra = (ReadArg*)arg;
    srand((unsigned)time(NULL));
    while (!ra->stop) {
        ra->c->wait_leader();
        braft::Node* leader = ra->c->leader();
        if (!leader) {
            continue;
        }

        braft::Node* read_node = NULL;
        if (ra->read_role == kLeaderRead) {
          read_node = leader;
        } else if (ra->read_role == kFollowerRead) {
          std::vector<braft::Node*> nodes;
          ra->c->followers(&nodes);
          read_node = nodes[rand()%nodes.size()];
        }

        // Get committed index from leader
        braft::NodeStatus leader_status;
        leader->get_status(&leader_status);
        int64_t committed_index = leader_status.committed_index;

        ReadOnlyType read_type = (ReadOnlyType)ra->read_type;
        butil::Status st = read_node->wait_linear_consistency(read_type);
        CHECK(st.ok());

        // Get applied index from read node
        braft::NodeStatus read_node_status;
        read_node->get_status(&read_node_status);
        int64_t known_applied_index = read_node_status.known_applied_index;

        CHECK(known_applied_index >= committed_index);

        if (read_cnt++ % 5000 == 0) {
            LOG(ERROR) << "read routine runs: " << read_cnt;
        }
    }
    return NULL;
}

TEST_P(LinearizableReadTest, read) {
    const uint64_t kMaxDelayApplyTimeUS = 1000;
    const uint64_t kRunTimeUS = 5 * 60 * 1000 * 1000;

    std::vector<braft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        if (i != 0 && !multi_peer_) {
            peer.role = braft::Role::LEARNER;
        }
        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        DelayApplyFSM* fsm = new DelayApplyFSM(kMaxDelayApplyTimeUS, peers[i].addr);
        ASSERT_EQ(0, cluster.start_with_statemachine(peers[i].addr, peers[i].role, fsm));
    }

    // elect leader_first
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    LOG(WARNING) << "start apply routine";
    // start apply routine
    ApplyArg apply_arg;
    apply_arg.c = &cluster;
    apply_arg.stop = false;
    pthread_t apply_tid;
    ASSERT_EQ(0, pthread_create(&apply_tid, NULL, apply_routine, &apply_arg));

    LOG(WARNING) << "start read routine";
    // start read routine
    ReadArg read_arg;
    read_arg.c = &cluster;
    read_arg.stop = false;
    read_arg.read_type = read_type_;
    read_arg.read_role = read_role_;
    pthread_t read_tid;
    ASSERT_EQ(0, pthread_create(&read_tid, NULL, read_routine, &read_arg));

    LOG(WARNING) << "wait routine exit";
    bthread_usleep(kRunTimeUS);

    apply_arg.stop = true;
    pthread_join(apply_tid, NULL);

    LOG(WARNING) << "apply routine exit";

    read_arg.stop = true;
    pthread_join(read_tid, NULL);

    LOG(WARNING) << "read routine exit";

    cluster.stop_all();
}

INSTANTIATE_TEST_CASE_P(LinearizableReadCheckQuorum,
                        LinearizableReadTest,
                        ::testing::Values(
                          std::make_tuple(0, 0, "LeaderRead"),
                          std::make_tuple(0, 0, "FollowerRead"),
                          std::make_tuple(0, 1, "LeaderRead"),
                          std::make_tuple(0, 1, "FollowerRead")
                        ));

INSTANTIATE_TEST_CASE_P(LinearizableReadLeaseBased,
                        LinearizableReadTest,
                        ::testing::Values(
                          std::make_tuple(1, 0, "LeaderRead"),
                          std::make_tuple(1, 0, "FollowerRead"),
                          std::make_tuple(1, 1, "LeaderRead"),
                          std::make_tuple(1, 1, "FollowerRead")
                        ));

int main(int argc, char* argv[]) {
    ::testing::AddGlobalTestEnvironment(new TestEnvironment());
    ::testing::InitGoogleTest(&argc, argv);
    GFLAGS_NS::SetCommandLineOption("minloglevel", "0");
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
