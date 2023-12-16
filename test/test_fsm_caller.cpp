// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/12/01 17:03:46

#include <gtest/gtest.h>
#include <butil/string_printf.h>
#include <butil/memory/scoped_ptr.h>
#include "braft/fsm_caller.h"
#include "braft/raft.h"
#include "braft/log.h"
#include "braft/configuration.h"
#include "braft/log_manager.h"

namespace braft {
DECLARE_bool(raft_sync);
}

class FSMCallerTest : public testing::Test {
protected:
    void SetUp() {}
    void TearDown() {}
};

class OrderedStateMachine : public braft::StateMachine {
public:
    OrderedStateMachine() 
        : _expected_next(0)
        , _stopped(false)
        , _on_leader_start_times(0)
        , _on_leader_stop_times(0)
        , _on_snapshot_save_times(0)
        , _on_snapshot_load_times(0)
    {}
    void on_apply(braft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            std::string expected;
            butil::string_printf(&expected, "hello_%" PRIu64, _expected_next++);
            ASSERT_EQ(expected, iter.data().to_string());
            if (iter.done()) {
                ASSERT_TRUE(iter.done()->status().ok()) << "index=" << iter.index();
                iter.done()->Run();
            }
        }
    }
    void on_shutdown() {
        _stopped = true;
    }
    void on_snapshot_save(braft::SnapshotWriter* /*writer*/, braft::Closure* done) {
        done->Run();
        ++_on_snapshot_save_times;
    }
    int on_snapshot_load(braft::SnapshotReader* /*reader*/) {
        ++_on_snapshot_load_times;
        return 0;
    }
    void on_leader_start(int64_t term) {
        _on_leader_start_times++;
    }
    virtual void on_leader_stop(const butil::Status& status) {
        _on_leader_stop_times++;
    }
    void join() {
        while (!_stopped) {
            bthread_usleep(100);
        }
    }
private:
    std::atomic<uint64_t> _expected_next;
    bool _stopped;
    int _on_leader_start_times;
    int _on_leader_stop_times;
    int _on_snapshot_save_times;
    int _on_snapshot_load_times;
};

class SyncClosure : public braft::LogManager::StableClosure {
public:
    SyncClosure() {
        _butex = bthread::butex_create_checked<butil::atomic<int> >();
        *_butex = 0;
    }
    ~SyncClosure() {
        bthread::butex_destroy(_butex);
    }
    void Run() {
        _butex->store(1);
        bthread::butex_wake(_butex);
    }
    void reset() {
        status().reset();
        *_butex = 0;
    }
    void join() {
        while (*_butex != 1) {
            bthread::butex_wait(_butex, 0, NULL);
        }
    }
private:
    butil::atomic<int> *_butex;
};

TEST_F(FSMCallerTest, sanity) {
    system("rm -rf ./data");
    scoped_ptr<braft::ConfigurationManager> cm(
                                new braft::ConfigurationManager);
    scoped_ptr<braft::SegmentLogStorage> storage(
                                new braft::SegmentLogStorage("./data"));
    scoped_ptr<braft::LogManager> lm(new braft::LogManager());
    braft::LogManagerOptions log_opt;
    log_opt.log_storage = storage.get();
    log_opt.configuration_manager = cm.get();
    ASSERT_EQ(0, lm->init(log_opt));

    braft::ClosureQueue cq(false);

    OrderedStateMachine fsm;
    fsm._expected_next = 0;

    braft::FSMCallerOptions opt;
    opt.log_manager = lm.get();
    opt.after_shutdown = NULL;
    opt.fsm = &fsm;
    opt.closure_queue = &cq;

    braft::FSMCaller caller;
    ASSERT_EQ(0, caller.init(opt));

    const size_t N = 1000;

    for (size_t i = 0; i < N; ++i) {
        std::vector<braft::LogEntry*> entries;
        braft::LogEntry* entry = new braft::LogEntry;
        entry->AddRef();
        entry->type = braft::ENTRY_TYPE_DATA;
        std::string buf;
        butil::string_printf(&buf, "hello_%lld", (long long)i);
        entry->data.append(buf);
        entry->id.index = i + 1;
        entry->id.term = i;
        entries.push_back(entry);
        SyncClosure c;
        lm->append_entries(&entries, &c);
        c.join();
        ASSERT_TRUE(c.status().ok()) << c.status();
    }
    ASSERT_EQ(0, caller.on_committed(N));
    ASSERT_EQ(0, caller.shutdown());
    fsm.join();
    ASSERT_EQ(fsm._expected_next, N);
}

class ExpectedValueLocalReadIndexClosure : public braft::LocalReadIndexClosure {
public:
    ExpectedValueLocalReadIndexClosure(OrderedStateMachine* state_machine)
        : _value(0), _state_machine(state_machine) {}

    void Run() {
        _value = _state_machine->_expected_next;
        braft::LocalReadIndexClosure::Run();
    }

    bool check_value() { return  _value >= index(); }

    void set_wait_id(braft::FSMCaller::WaitId wait_id) { _wait_id = wait_id; }
    braft::FSMCaller::WaitId wait_id() const { return _wait_id; }

private:
    int64_t _value;
    braft::FSMCaller::WaitId _wait_id;
    OrderedStateMachine* _state_machine;
};

struct WaitIdAndClosure {
    WaitIdAndClosure(braft::FSMCaller::WaitId _wait_id, braft::LocalReadIndexClosure* _done)
        : wait_id(_wait_id), done(_done) {}

    braft::FSMCaller::WaitId wait_id;
    braft::LocalReadIndexClosure* done;
};

struct WaitArg {
    braft::FSMCaller* caller;
    OrderedStateMachine* state_machine;
    int64_t max_index;
};

static void* wait_routine(void* arg) {
    const size_t N = 100000;
    std::vector<ExpectedValueLocalReadIndexClosure*> wait_closures;
    wait_closures.reserve(N);

    WaitArg* wa = (WaitArg*)arg;
    srand((unsigned)time(NULL));
    for (size_t i = 0; i < N; ++i) {
        ExpectedValueLocalReadIndexClosure* closure = new ExpectedValueLocalReadIndexClosure(wa->state_machine);
        closure->set_index((rand()%wa->max_index) + 1);
        wait_closures.push_back(closure);
        closure->set_wait_id(wa->caller->wait_on_apply(closure));
    }

    LOG(ERROR) << "append closure done";

    int no_wait = 0;
    for (size_t i = 0; i < N; ++i) {
        wait_closures[i]->wait();
        CHECK(wait_closures[i]->check_value());
        if (wait_closures[i]->wait_id() == -1) {
          no_wait++;
        }
        delete wait_closures[i];
        if (i % (N/10) == 0) {
            LOG(ERROR) << "wait ready number: " << i;
        }
    }

    LOG(ERROR) << "wait and check closure done, no wait size: " << no_wait;

    return NULL;
}

TEST_F(FSMCallerTest, apply_sanity) {
    system("rm -rf ./data");
    scoped_ptr<braft::ConfigurationManager> cm(
                                new braft::ConfigurationManager);
    scoped_ptr<braft::SegmentLogStorage> storage(
                                new braft::SegmentLogStorage("./data"));
    scoped_ptr<braft::LogManager> lm(new braft::LogManager());
    braft::LogManagerOptions log_opt;
    log_opt.log_storage = storage.get();
    log_opt.configuration_manager = cm.get();
    ASSERT_EQ(0, lm->init(log_opt));

    braft::ClosureQueue cq(false);

    OrderedStateMachine fsm;
    fsm._expected_next = 0;

    braft::FSMCallerOptions opt;
    opt.log_manager = lm.get();
    opt.after_shutdown = NULL;
    opt.fsm = &fsm;
    opt.closure_queue = &cq;

    braft::FSMCaller caller;
    ASSERT_EQ(0, caller.init(opt));

    const size_t N = 1000000;
    srand((unsigned)time(NULL));

    std::vector<WaitIdAndClosure> wait_closures;
    wait_closures.reserve(N);
    for (size_t i = 0; i < N; ++i) {
        braft::LocalReadIndexClosure* closure = new braft::LocalReadIndexClosure();
        closure->set_index((rand()%10000) + 1);

        braft::FSMCaller::WaitId wait_id = caller.wait_on_apply(closure);
        ASSERT_NE(wait_id, -1);
        wait_closures.push_back(WaitIdAndClosure(wait_id, closure));
    }

    for (size_t i = 0; i < wait_closures.size(); ++i) {
        ASSERT_EQ(0, caller.remove_waiter(wait_closures[i].wait_id));
        wait_closures[i].done->wait();
        ASSERT_EQ(wait_closures[i].done->status().error_code(), EIDRM);
        delete wait_closures[i].done;
    }

    ASSERT_EQ(0, caller.shutdown());
    fsm.join();
}

TEST_F(FSMCallerTest, apply_waiter) {
    braft::FLAGS_raft_sync = false;
    system("rm -rf ./data");
    scoped_ptr<braft::ConfigurationManager> cm(
                                new braft::ConfigurationManager);
    scoped_ptr<braft::SegmentLogStorage> storage(
                                new braft::SegmentLogStorage("./data"));
    scoped_ptr<braft::LogManager> lm(new braft::LogManager());
    braft::LogManagerOptions log_opt;
    log_opt.log_storage = storage.get();
    log_opt.configuration_manager = cm.get();
    ASSERT_EQ(0, lm->init(log_opt));

    braft::ClosureQueue cq(false);

    OrderedStateMachine fsm;
    fsm._expected_next = 0;

    braft::FSMCallerOptions opt;
    opt.log_manager = lm.get();
    opt.after_shutdown = NULL;
    opt.fsm = &fsm;
    opt.closure_queue = &cq;

    braft::FSMCaller caller;
    ASSERT_EQ(0, caller.init(opt));

    const size_t N = 50000;

    WaitArg arg;
    arg.caller = &caller;
    arg.state_machine = &fsm;
    arg.max_index = N;
    pthread_t tid;
    ASSERT_EQ(0, pthread_create(&tid, NULL, wait_routine, &arg));

    LOG(ERROR) << "start to append entries";

    for (size_t i = 0; i < N; ++i) {
        std::vector<braft::LogEntry*> entries;
        braft::LogEntry* entry = new braft::LogEntry;
        entry->AddRef();
        entry->type = braft::ENTRY_TYPE_DATA;
        std::string buf;
        butil::string_printf(&buf, "hello_%lld", (long long)i);
        entry->data.append(buf);
        entry->id.index = i + 1;
        entry->id.term = i;
        entries.push_back(entry);
        SyncClosure c;
        lm->append_entries(&entries, &c);
        c.join();
        ASSERT_TRUE(c.status().ok()) << c.status();
        ASSERT_EQ(0, caller.on_committed(i + 1));
        if (i % (N/10) == 0) {
            LOG(ERROR) << "append entries number: " << i;
        }
    }
    LOG(ERROR) << "append entries done";

    pthread_join(tid, NULL);

    ASSERT_EQ(0, caller.shutdown());
    fsm.join();
    ASSERT_EQ(fsm._expected_next, N);
}

TEST_F(FSMCallerTest, on_leader_start_and_stop) {
    scoped_ptr<braft::LogManager> lm(new braft::LogManager());
    OrderedStateMachine fsm;
    fsm._expected_next = 0;
    braft::ClosureQueue cq(false);
    braft::FSMCallerOptions opt;
    opt.log_manager = lm.get();
    opt.after_shutdown = NULL;
    opt.fsm = &fsm;
    opt.closure_queue = &cq;
    braft::FSMCaller caller;
    ASSERT_EQ(0, caller.init(opt));
    butil::Status status;
    caller.on_leader_stop(status);
    caller.shutdown();
    fsm.join();
    ASSERT_EQ(0, fsm._on_leader_start_times);
    ASSERT_EQ(1, fsm._on_leader_stop_times);
}

class DummySnapshotReader : public braft::SnapshotReader {
public:
    DummySnapshotReader(braft::SnapshotMeta* meta) 
        : _meta(meta)
    {
    };
    ~DummySnapshotReader() {}
    std::string generate_uri_for_copy() { return std::string(); }
    void list_files(std::vector<std::string>*) {}
    int get_file_meta(const std::string&, google::protobuf::Message*) { return 0; }
    std::string get_path() { return std::string(); }
    int load_meta(braft::SnapshotMeta* meta) {
        *meta = *_meta;
        return 0;
    }
private:
    braft::SnapshotMeta* _meta;
};

class DummySnapshoWriter : public braft::SnapshotWriter {
public:
    DummySnapshoWriter() {}
    ~DummySnapshoWriter() {}
    int save_meta(const braft::SnapshotMeta&) {
        EXPECT_TRUE(false) << "Should never be called";
        return 0;
    }
    std::string get_path() { return std::string(); }
    int add_file(const std::string&, const google::protobuf::Message*) { return 0;}
    int remove_file(const std::string&) { return 0; }
    void list_files(std::vector<std::string>*) {}
    int get_file_meta(const std::string&, google::protobuf::Message*) { return 0; }
private:
};

class MockSaveSnapshotClosure : public braft::SaveSnapshotClosure {
public:
    MockSaveSnapshotClosure(braft::SnapshotWriter* writer, 
                            braft::SnapshotMeta *expected_meta) 
        : _start_times(0)
        , _writer(writer)
        , _expected_meta(expected_meta)
    {
    }
    ~MockSaveSnapshotClosure() {}
    void Run() {
        ASSERT_TRUE(status().ok()) << status();
    }
    braft::SnapshotWriter* start(const braft::SnapshotMeta& meta) {
        EXPECT_EQ(meta.last_included_index(), 
                    _expected_meta->last_included_index());
        EXPECT_EQ(meta.last_included_term(), 
                    _expected_meta->last_included_term());
        ++_start_times;
        return _writer;
    }
private:
    int _start_times;
    braft::SnapshotWriter* _writer;
    braft::SnapshotMeta* _expected_meta;
};

class MockLoadSnapshotClosure : public braft::LoadSnapshotClosure {
public:
    MockLoadSnapshotClosure(braft::SnapshotReader* reader)
        : _start_times(0)
        , _reader(reader)
    {}
    ~MockLoadSnapshotClosure() {}
    void Run() {
        ASSERT_TRUE(status().ok()) << status();
    }
    braft::SnapshotReader* start() {
        ++_start_times;
        return _reader;
    }
private:
    int _start_times;
    braft::SnapshotReader* _reader;
};

TEST_F(FSMCallerTest, snapshot) {
    braft::SnapshotMeta snapshot_meta;
    snapshot_meta.set_last_included_index(0);
    snapshot_meta.set_last_included_term(0);
    DummySnapshotReader dummy_reader(&snapshot_meta);
    DummySnapshoWriter dummy_writer;
    MockSaveSnapshotClosure save_snapshot_done(&dummy_writer, &snapshot_meta);
    system("rm -rf ./data");
    scoped_ptr<braft::ConfigurationManager> cm(
                                new braft::ConfigurationManager);
    scoped_ptr<braft::SegmentLogStorage> storage(
                                new braft::SegmentLogStorage("./data"));
    scoped_ptr<braft::LogManager> lm(new braft::LogManager());
    braft::LogManagerOptions log_opt;
    log_opt.log_storage = storage.get();
    log_opt.configuration_manager = cm.get();
    ASSERT_EQ(0, lm->init(log_opt));

    OrderedStateMachine fsm;
    fsm._expected_next = 0;
    braft::ClosureQueue cq(false);
    braft::FSMCallerOptions opt;
    opt.log_manager = lm.get();
    opt.after_shutdown = NULL;
    opt.fsm = &fsm;
    opt.closure_queue = &cq;
    braft::FSMCaller caller;
    ASSERT_EQ(0, caller.init(opt));
    ASSERT_EQ(0, caller.on_snapshot_save(&save_snapshot_done));
    MockLoadSnapshotClosure load_snapshot_done(&dummy_reader);
    ASSERT_EQ(0, caller.on_snapshot_load(&load_snapshot_done));
    ASSERT_EQ(0, caller.shutdown());
    fsm.join();
    ASSERT_EQ(1, fsm._on_snapshot_save_times);
    ASSERT_EQ(1, fsm._on_snapshot_load_times);
    ASSERT_EQ(1, save_snapshot_done._start_times);
    ASSERT_EQ(1, load_snapshot_done._start_times);
}

