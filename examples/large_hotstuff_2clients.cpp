/**
 * Copyright 2018 VMware
 * Copyright 2018 Ted Yin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>
#include <random>
#include <signal.h>
#include <sys/time.h>

#include "salticidae/type.h"
#include "salticidae/netaddr.h"
#include "salticidae/network.h"
#include "salticidae/util.h"

#include "hotstuff/util.h"
#include "hotstuff/type.h"
#include "hotstuff/client.h"

using salticidae::Config;

using hotstuff::ReplicaID;
using hotstuff::NetAddr;
using hotstuff::EventContext;
using hotstuff::MsgReqCmd;
using hotstuff::MsgEstConnReqCmd;
using hotstuff::MsgEstConnRespCmd;

using hotstuff::MsgOrdering1ReqCmd;
using hotstuff::MsgOrdering2ReqCmd;
using hotstuff::MsgRespCmd;
using hotstuff::MsgOrdering1RespCmd;
using hotstuff::MsgOrdering2RespCmd;
using hotstuff::MsgConsensusRespClientCmd;
using hotstuff::CommandDummy;
using hotstuff::HotStuffError;
using hotstuff::uint256_t;
using hotstuff::opcode_t;
using hotstuff::command_t;

EventContext ec;
ReplicaID proposer;
size_t max_async_num;
int max_iter_num;
uint32_t cid;
uint32_t cnt = 0;
uint32_t nfaulty;

struct Request {
    bool strong;

    command_t cmd;
    size_t confirmed;
    int idx, height, index;
    salticidae::ElapsedTime et;
    //    std::vector<std::string> timestamps;
    Request(int cnt, const command_t &cmd, bool strong): idx(cnt), cmd(cmd), strong(strong), confirmed(0)
    {
        et.start();
    }
};

bool request_smaller(const struct Request& left, const struct Request& right) {
    return left.idx < right.idx;
}

int BATCH_SIZE, STABLE_PERIOD;
const int max_waiting_exec = 500;
int count_sent, count_weak, count_strong, count_backoff;
using Net = salticidae::MsgNetwork<opcode_t>;

std::unordered_map<ReplicaID, Net::conn_t> weak_conns;
std::unordered_map<ReplicaID, Net::conn_t> strong_conns;
std::vector<Request> weak_finished, strong_finished;
std::unordered_map<const uint256_t, Request> waiting, waiting_exec;
std::vector<NetAddr> replicas, strong_replicas;
std::vector<std::pair<struct timeval, double>> elapsed, elapsed_exec;
Net mn(ec, Net::Config());

void preferences_stats(const char*, std::vector<Request>&);
void connect_all() {
    for (size_t i = 0; i < replicas.size(); i++)
        weak_conns.insert(std::make_pair(i, mn.connect_sync(replicas[i])));
}

void connect_all_strong() {
    for (size_t i = 0; i < strong_replicas.size(); i++)
        strong_conns.insert(std::make_pair(i, mn.connect_sync(strong_replicas[i])));
}


//static int debug_limit = 0;
static int debug_leader = 0;
bool try_send(bool check = true) {
    //if (debug_limit++ > 10) return false;

    if ((!check || waiting.size() < max_async_num) && max_iter_num)
    {
        int start_cnt = cnt;

        // Weak client's command
        auto cmd0 = new CommandDummy(cid, cnt++);
        MsgReqCmd msg0(*cmd0, debug_leader);
        for (int i = 0; i < BATCH_SIZE; i++) {
            for (auto &p: weak_conns) mn.send_msg(msg0, p.second);
        }
        // Instead, only send to the proposer
        // mn.send_msg(msg0, weak_conns[0]);
        waiting.insert(std::make_pair(cmd0->get_hash(), Request(start_cnt, cmd0, false)));

        // Strong client's command
        auto cmd1 = new CommandDummy(cid, cnt++);
        MsgReqCmd msg1(*cmd1, debug_leader);
        for (int i = 0; i < BATCH_SIZE; i++) {
            for (auto &p: strong_conns) mn.send_msg(msg1, p.second);
        }
        // Instead, only send to the proposer
        // mn.send_msg(msg0, strong_conns[0]);
        waiting.insert(std::make_pair(cmd1->get_hash(), Request(start_cnt, cmd1, true)));
        debug_leader = (debug_leader + 1) % weak_conns.size();

#ifndef HOTSTUFF_ENABLE_BENCHMARK
        HOTSTUFF_LOG_INFO("send new cmd %.10s",
                            get_hex(cmd->get_hash()).c_str());
#endif
        if (max_iter_num > 0)
            max_iter_num--;
        return true;
    }
    return false;
}

void client_resp_cmd_handler(MsgRespCmd &&msg, const Net::conn_t &) {
    auto &fin = msg.fin;
    //HOTSTUFF_LOG_DEBUG("got %s", std::string(msg.fin).c_str());
    const uint256_t &cmd_hash = fin.cmd_hash;
    auto it = waiting.find(cmd_hash);
    auto &et = it->second.et;
    if (it == waiting.end()) return;

    //if (it->second.confirmed++ > 0) return; // wait for only 1 ack, from the proposer
    if (++it->second.confirmed <= nfaulty) return; // wait for f + 1 ack
    et.stop();

    if (it->second.strong)
        count_strong++;
    else
        count_weak++;

#ifndef HOTSTUFF_ENABLE_BENCHMARK
    HOTSTUFF_LOG_INFO("got %s, wall: %.3f, cpu: %.3f",
                        std::string(fin).c_str(),
                        et.elapsed_sec, et.cpu_elapsed_sec);
#else
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    elapsed.push_back(std::make_pair(tv, et.elapsed_sec));
#endif

    it->second.index = fin.cmd_idx;
    it->second.height = fin.cmd_height;
    if (it->second.strong)
        strong_finished.push_back(it->second);
    else
        weak_finished.push_back(it->second);

    waiting.erase(it);
    while (try_send());
}


std::pair<std::string, std::string> split_ip_port_cport(const std::string &s) {
    auto ret = salticidae::trim_all(salticidae::split(s, ";"));
    return std::make_pair(ret[0], ret[1]);
}

int main(int argc, char **argv) {
    // Parse speedbumps for the strong client
    Config config_strong(argv[2]);
    auto opt_strong_replicas = Config::OptValStrVec::create();
    config_strong.add_opt("replica", opt_strong_replicas, Config::APPEND);
    config_strong.parse(1, argv);
    std::vector<std::string> raw_strong;
    for (const auto &s: opt_strong_replicas->get())
    {
        auto res = salticidae::trim_all(salticidae::split(s, ","));
        if (res.size() < 1)
            throw HotStuffError("format error");
        raw_strong.push_back(res[0]);
    }
    for (const auto &p: raw_strong)
    {
        auto _p = split_ip_port_cport(p);
        size_t _;
        strong_replicas.push_back(NetAddr(NetAddr(_p.first).ip, htons(stoi(_p.second, &_))));
        //printf("Pompe-unbias-client: strong bump %s\n", _p.first.c_str());
    }

    // Parse speedbumps for the weak client and other configuration
    Config config(argv[1]);
    std::string orderlogfile(argv[3]);
    std::string execlogfile(argv[4]);
    //Config config("hotstuff.conf");

    auto opt_blk_size = Config::OptValInt::create(1);
    auto opt_stable_period = Config::OptValInt::create(50);
    auto opt_idx = Config::OptValInt::create(0);
    auto opt_replicas = Config::OptValStrVec::create();
    auto opt_max_iter_num = Config::OptValInt::create(-1);
    auto opt_max_async_num = Config::OptValInt::create(5);
    auto opt_cid = Config::OptValInt::create(-1);

    auto shutdown = [&](int) { ec.stop(); };
    salticidae::SigEvent ev_sigint(ec, shutdown);
    salticidae::SigEvent ev_sigterm(ec, shutdown);
    ev_sigint.add(SIGINT);
    ev_sigterm.add(SIGTERM);

    mn.reg_handler(client_resp_cmd_handler);
    mn.start();

    config.add_opt("block-size", opt_blk_size, Config::SET_VAL);
    config.add_opt("stable-period", opt_stable_period, Config::SET_VAL);
    config.add_opt("idx", opt_idx, Config::SET_VAL);
    config.add_opt("cid", opt_cid, Config::SET_VAL);
    config.add_opt("replica", opt_replicas, Config::APPEND);
    config.add_opt("iter", opt_max_iter_num, Config::SET_VAL);
    config.add_opt("max-async", opt_max_async_num, Config::SET_VAL);
    config.parse(argc, argv);

    BATCH_SIZE = opt_blk_size->get();
    STABLE_PERIOD = opt_stable_period->get() * 1000;
    auto idx = opt_idx->get();
    max_iter_num = opt_max_iter_num->get();
    max_async_num = opt_max_async_num->get();
    std::vector<std::string> raw;
    for (const auto &s: opt_replicas->get())
    {
        auto res = salticidae::trim_all(salticidae::split(s, ","));
        if (res.size() < 1)
            throw HotStuffError("format error");
        raw.push_back(res[0]);
    }

    if (!(0 <= idx && (size_t)idx < raw.size() && raw.size() > 0))
        throw std::invalid_argument("out of range");
    cid = opt_cid->get() != -1 ? opt_cid->get() : idx;
    for (const auto &p: raw)
    {
        auto _p = split_ip_port_cport(p);
        size_t _;
        replicas.push_back(NetAddr(NetAddr(_p.first).ip, htons(stoi(_p.second, &_))));
    }

    nfaulty = (replicas.size() - 1) / 3;
    HOTSTUFF_LOG_INFO("nfaulty = %zu", nfaulty);
    connect_all();
    connect_all_strong();

    while (try_send());
    ec.dispatch();

#ifdef HOTSTUFF_ENABLE_BENCHMARK

    // printf("client write to order log file %s, %lu entries\n", orderlogfile.c_str(), elapsed.size());
    //printf("client write to exec log file %s, %lu entries\n", execlogfile.c_str(), elapsed_exec.size());
    printf("[DEBUG] client%d receives %d consensus responses, %d weak + %d strong\n", cid, elapsed.size(), count_weak, count_strong);

    std::sort(weak_finished.begin(), weak_finished.end(), request_smaller);
    std::sort(strong_finished.begin(), strong_finished.end(), request_smaller);

    int weak_score(0), strong_score(0);
    int total = weak_finished.size();
    if (total > strong_finished.size())
        total = strong_finished.size();
    for (int i = 0; i < total; i++) {
        // if (i < 5)
        //     printf("[RESULT] idx=%d, strong=%d, weak=%d\n", strong_finished[i].idx/2, strong_finished[i].height, weak_finished[i].height);
        assert(strong_finished[i].idx == weak_finished[i].idx);

        if (strong_finished[i].height < weak_finished[i].height)
            strong_score++;
        else
            weak_score++;
    }
    printf("[RESULT] strong score=%d, weak score=%d, total=%d\n", strong_score, weak_score, total);

    // for (int i = 0; i < 5; i++) {
    //     printf("[DEBUG] Weak client round%d, f+1=%d\n", i, nfaulty*2+1);
    //     for (int j = 0; j < nfaulty*2+1; j++)
    //         printf("  %lu", weak_finished[j].median_delta);
    //     printf("\n");
    // }
    
    //preferences_stats("Poor", weak_finished);
    //preferences_stats("Rich", strong_finished);

    // freopen(execlogfile.c_str(), "w", stdout);

    // for (const auto &e: elapsed_exec)
    // {
    //     char fmt[64];
    //     struct tm *tmp = localtime(&e.first.tv_sec);
    //     strftime(fmt, sizeof fmt, "%Y-%m-%d %H:%M:%S.%%06u [hotstuff info] %%.6f\n", tmp);
    //     fprintf(stdout, fmt, e.first.tv_usec, e.second);
    // }

    
    // freopen(orderlogfile.c_str(), "w", stdout);

    // invocation -> receive
    // for (int i = 0; i < finished.size(); i++) {
    //     int64_t invocation = finished[i].invocation_time_us;
    //     for (int j = 0; j < finished[i].recv_timestamps.size(); j++)
    //         printf("%ld    ", (int64_t)finished[i].recv_timestamps[j] - invocation);
    //     printf("\n");
    // }
    // for (const auto &e: elapsed)
    // {
    //     char fmt[64];
    //     struct tm *tmp = localtime(&e.first.tv_sec);
    //     strftime(fmt, sizeof fmt, "%Y-%m-%d %H:%M:%S.%%06u [hotstuff info] %%.6f\n", tmp);
    //     fprintf(stdout, fmt, e.first.tv_usec, e.second);
    // }


    fclose(stdout);

#endif
    return 0;
}
