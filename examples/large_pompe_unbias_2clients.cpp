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
//using hotstuff::MsgReqCmd;
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
    command_t match_cmd;
    size_t confirmed;
    size_t estconn_rtt;
    size_t ordering_rtt1;
    size_t ordering_rtt2;
    size_t ordering_rtt3;
    salticidae::ElapsedTime et;
    salticidae::ElapsedTime et_exec;
    //    std::vector<std::string> timestamps;
    uint64_t conn_time_us, invoc_time_us;
    std::vector<uint64_t> conn_timestamps;
    std::vector<uint64_t> recv_timestamps;
    Request(const command_t &cmd, bool strong): cmd(cmd), strong(strong), confirmed(0), estconn_rtt(0), ordering_rtt1(0), ordering_rtt2(0), ordering_rtt3(0)
    {
        et.start();
        et_exec.start();
        invoke();
    }

    void invoke() { invoc_time_us = now(); }

    uint64_t now() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        uint64_t ret = tv.tv_sec * 1000 * 1000;
        return ret + tv.tv_usec;
    }
};

int BATCH_SIZE, STABLE_PERIOD;
const int max_waiting_exec = 500;
int count_sent, count_order, count_exec, count_backoff;
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
bool try_send(bool check = true) {
    //if (debug_limit++ > 10) return false;

    if ((!check || waiting.size() < max_async_num) && max_iter_num)
    {
        // Weak client's command
        auto cmd0 = new CommandDummy(cid, cnt++);
        //MsgOrdering1ReqCmd msg0(*cmd0);
        MsgEstConnReqCmd msg0(*cmd0);
        for (int i = 0; i < BATCH_SIZE; i++) {
            for (auto &p: weak_conns) mn.send_msg(msg0, p.second);
        }
        waiting.insert(std::make_pair(cmd0->get_hash(), Request(cmd0, false)));

        // Strong client's command
        auto cmd1 = new CommandDummy(cid, cnt++);
        //MsgOrdering1ReqCmd msg1(*cmd1);
        MsgEstConnReqCmd msg1(*cmd1);
        for (int i = 0; i < BATCH_SIZE; i++) {
            for (auto &p: strong_conns) mn.send_msg(msg1, p.second);
        }
        waiting.insert(std::make_pair(cmd1->get_hash(), Request(cmd1, true)));

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

void client_estconn_resp_cmd_handler(MsgEstConnRespCmd &&msg, const Net::conn_t &conn) {
    //printf("[TMP] client receives EstConnResp\n");

    const uint256_t &cmd_hash = msg.cmd_hash;
    auto it = waiting.find(cmd_hash);
    if (it == waiting.end()) return;

    it->second.conn_timestamps.push_back(msg.timestamp_us);
    if (++it->second.estconn_rtt != nfaulty*2+1) return; // barrier for connection establishment
    
    // send the first rtt message of ordering phase
    it->second.invoke(); // get invocation time
    MsgOrdering1ReqCmd next_msg(*it->second.cmd);

    if (it->second.strong) {
        // Strong client
        for (auto &p: strong_conns) {
            mn.send_msg(next_msg, p.second);
        }
    } else {
        // Weak client
        for (auto &p: weak_conns) {
            mn.send_msg(next_msg, p.second);
        }
    }
}

void client_ordering1_resp_cmd_handler(MsgOrdering1RespCmd &&msg, const Net::conn_t &) {
    //HOTSTUFF_LOG_DEBUG("got %s", std::string(msg.fin).c_str());
    const uint256_t &cmd_hash = msg.cmd_hash;
    auto it = waiting.find(cmd_hash);
    if (it == waiting.end()) return;
    auto &et = it->second.et;    

    //std::string t = std::string(get_hex10(msg.timestamp));
    it->second.recv_timestamps.push_back(msg.timestamp_us);
    if (++it->second.ordering_rtt1 != nfaulty*2+1) return; // wait for 2f + 1 timestamps

    // pick the median timestamp, the f+1 th one
    std::sort(it->second.recv_timestamps.begin(), it->second.recv_timestamps.end());
    uint64_t median = it->second.recv_timestamps[nfaulty + 1];
    
    // send the second rtt message of ordering phase
    MsgOrdering2ReqCmd next_msg(cmd_hash, median);

    if (it->second.strong) {
        // Rich client
        for (auto &p: strong_conns) {
            mn.send_msg(next_msg, p.second);
        }
    } else {
        // Poor client
        for (auto &p: weak_conns) {
            mn.send_msg(next_msg, p.second);
        }
    }
}

void client_ordering2_resp_cmd_handler(MsgOrdering2RespCmd &&msg, const Net::conn_t &) {
    //HOTSTUFF_LOG_DEBUG("got %s", std::string(msg.fin).c_str());
    const uint256_t &cmd_hash = msg.cmd_hash;
    auto it = waiting.find(cmd_hash);
    if (it == waiting.end()) return;
    auto &et = it->second.et;
    
    if (++it->second.ordering_rtt2 != nfaulty*2+1) return; // wait for 2f + 1 ack
    et.stop();

#ifndef HOTSTUFF_ENABLE_BENCHMARK
    HOTSTUFF_LOG_INFO("got %s, wall: %.3f, cpu: %.3f, timestamps: %s",
                        std::string(get_hex10(cmd_hash)).c_str(),
                      et.elapsed_sec, et.cpu_elapsed_sec),
                        std::string(get_hex10(msg.timestamp)).c_str();
#else
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    //elapsed.push_back(std::make_pair(tv, et.elapsed_sec));
    count_order++;
    for (int i = 0; i < BATCH_SIZE; i++)
        elapsed.push_back(std::make_pair(tv, et.elapsed_sec));

    // for debug
    //fprintf(stdout, "got %s, timestamps: %s\n", std::string(get_hex10(cmd_hash)).c_str(), std::string(get_hex10(msg.timestamp)).c_str());
#endif
    if (it->second.strong) {
        // Rich client
        strong_finished.push_back(it->second);
    } else {
        // Poor client
        weak_finished.push_back(it->second);
    }
    waiting_exec.insert(std::make_pair(it->first, it->second));
    waiting.erase(it);

    while (try_send());
}

void client_ordering_exec_resp_handler(MsgConsensusRespClientCmd &&msg, const Net::conn_t &) {
    //HOTSTUFF_LOG_DEBUG("got %s", std::string(msg.fin).c_str());
    const uint256_t &cmd_hash = msg.cmd_hash;
    auto it = waiting_exec.find(cmd_hash);
    if (it == waiting_exec.end()) return;
    auto &et_exec = it->second.et_exec;

    if (++it->second.ordering_rtt3 != 1) return; // wait for 1 exec ack
    count_exec++;
    et_exec.stop();

#ifndef HOTSTUFF_ENABLE_BENCHMARK
    // HOTSTUFF_LOG_INFO("executed %s, wall: %.3f, cpu: %.3f",
    //                     std::string(get_hex10(cmd_hash)).c_str(),
    //                   et.elapsed_sec, et.cpu_elapsed_sec)).c_str();
#else
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    //elapsed_exec.push_back(std::make_pair(tv, et_exec.elapsed_sec));
    for (int i = 0; i < BATCH_SIZE; i++)
        elapsed_exec.push_back(std::make_pair(tv, et_exec.elapsed_sec));

    // for debug
    //fprintf(stdout, "got %s, timestamps: %s\n", std::string(get_hex10(cmd_hash)).c_str(), "303030000");
#endif
    waiting_exec.erase(it);

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

    mn.reg_handler(client_estconn_resp_cmd_handler);
    mn.reg_handler(client_ordering1_resp_cmd_handler);
    mn.reg_handler(client_ordering2_resp_cmd_handler);
    mn.reg_handler(client_ordering_exec_resp_handler);
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

    printf("client write to order log file %s, %lu entries\n", orderlogfile.c_str(), elapsed.size());
    //printf("client write to exec log file %s, %lu entries\n", execlogfile.c_str(), elapsed_exec.size());
    printf("[DEBUG] client%d receives %d ordering, %d consensus responses\n", cid, elapsed.size(), count_exec);

    preferences_stats("Weak", weak_finished);
    preferences_stats("Strong", strong_finished);

    freopen(execlogfile.c_str(), "w", stdout);

    for (const auto &e: elapsed_exec)
    {
        char fmt[64];
        struct tm *tmp = localtime(&e.first.tv_sec);
        strftime(fmt, sizeof fmt, "%Y-%m-%d %H:%M:%S.%%06u [hotstuff info] %%.6f\n", tmp);
        fprintf(stdout, fmt, e.first.tv_usec, e.second);
    }

    
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


void preferences_stats(const char* type, std::vector<Request>& finished) {
    int finished_len = 100; // Get statistics of the first 100 invocations
    int avg_est = 0;
    // std::vector<int64_t> msg_delay(replicas.size());
    for (int i = 0; i < finished_len; i++) {
        std::sort(finished[i].conn_timestamps.begin(), finished[i].conn_timestamps.end());
        std::sort(finished[i].recv_timestamps.begin(), finished[i].recv_timestamps.end());

        std::vector<int64_t> unbiased;
        for (int j = 0; j < finished[i].recv_timestamps.size(); j++) {
            unbiased.push_back((finished[i].recv_timestamps[j] + finished[i].conn_timestamps[j]) / 2);
            // DEBUG
            // msg_delay[j] += finished[i].recv_timestamps[j] - invocation;
            // if (i == 0) {
            //     printf("[DEBUG] replica%d, recv %ld, conn %ld\n", j, finished[i].recv_timestamps[j], finished[i].conn_timestamps[j]);
            // }
        }

        std::sort(unbiased.begin(), unbiased.end());
        int64_t invocation = finished[i].invoc_time_us;
        avg_est += (invocation - unbiased[nfaulty + 1]);
        printf("[DEBUG] invocation %ld, unbiased %ld\n", invocation, unbiased[nfaulty + 1]);
    }
    avg_est /= finished_len;

    printf("    %s client: Average aggregate-to-invoke is t %ld ms %ld us\n", type, avg_est / 1000, avg_est % 1000);
    // printf("%s client: Average preferences from the first %d invocations\n", type, finished_len);
    // for (auto it : invoke_to_pref) {
    //     int64_t delta = it / finished_len;
    //     printf("    %ldms : %ldus\n", delta / 1000, delta % 1000);
    // }

    // printf("[DEBUG] %s client: single message delay from %d invocations\n", type, finished_len);
    // for (auto it : msg_delay) {
    //     int64_t delta = it / finished_len;
    //     printf("    [DEBUG] %ldms : %ldus\n", delta / 1000, delta % 1000);
    // }
}
