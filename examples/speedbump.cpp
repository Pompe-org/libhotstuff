#include <cstdio>

#include "salticidae/stream.h"
#include "salticidae/util.h"
#include "salticidae/network.h"
#include "salticidae/msg.h"

#include "hotstuff/promise.hpp"
#include "hotstuff/type.h"
#include "hotstuff/entity.h"
#include "hotstuff/util.h"
#include "hotstuff/client.h"
#include "hotstuff/hotstuff.h"
#include "hotstuff/liveness.h"

using salticidae::MsgNetwork;
using salticidae::ClientNetwork;
using salticidae::ElapsedTime;
using salticidae::Config;
using salticidae::_1;
using salticidae::_2;
using salticidae::static_pointer_cast;
using salticidae::trim_all;
using salticidae::split;

using hotstuff::TimerEvent;
using hotstuff::EventContext;
using hotstuff::NetAddr;
using hotstuff::HotStuffError;
using hotstuff::CommandDummy;
using hotstuff::Finality;
using hotstuff::command_t;
using hotstuff::uint256_t;
using hotstuff::opcode_t;
using hotstuff::bytearray_t;
using hotstuff::DataStream;
using hotstuff::ReplicaID;
using hotstuff::MsgReqCmd;
using hotstuff::MsgRespCmd;
using hotstuff::get_hash;
using hotstuff::promise_t;

using HotStuff = hotstuff::HotStuffSecp256k1;


class ClientSide {
    int idx;
    EventContext ec;
    EventContext req_ec;
    std::thread req_thread;
    ClientNetwork<opcode_t> cn;
    salticidae::BoxObj<salticidae::ThreadCall> req_tcall;
    using conn_t = ClientNetwork<opcode_t>::conn_t;

    static command_t parse_cmd(DataStream &s) {
        auto cmd = new CommandDummy();
        s >> *cmd;
        return cmd;
    }

    void client_req_handler(MsgReqCmd &&msg, const conn_t &conn) {
        const NetAddr addr = conn->get_addr();
        auto cmd = parse_cmd(msg.serialized);
        const auto &cmd_hash = cmd->get_hash();
        printf("Bump #%d processing %s\n", idx, std::string(*cmd).c_str());
        // exec_command(cmd_hash, [this, addr](Finality fin) {
        //     resp_queue.enqueue(std::make_pair(fin, addr));
        // });
    }
public:
    ClientSide(int idx,
               const EventContext &ec,
               NetAddr clisten_addr,
               const ClientNetwork<opcode_t>::Config &clinet_config):
        ec(ec),
        idx(idx),
        cn(req_ec, clinet_config) {

        req_tcall = new salticidae::ThreadCall(req_ec);

        cn.reg_handler(salticidae::generic_bind(&ClientSide::client_req_handler, this, _1, _2));
        cn.start();
        cn.listen(clisten_addr);

        req_thread = std::thread([this]() { printf("Bump#%d is in the req thread!\n", this->idx);req_ec.dispatch(); });
        //while(1);
    }

    void stop() {
        req_ec.stop();
        //req_thread.join();
        ec.stop();
    }
};

std::pair<std::string, std::string> split_ip_port_cport(const std::string &s) {
    auto ret = trim_all(split(s, ";"));
    if (ret.size() != 2)
        throw std::invalid_argument("invalid cport format");
    return std::make_pair(ret[0], ret[1]);
}

int main(int argc, char **argv) {
    std::string config_node_file(argv[2]);
    Config config_bump(argv[1]);
    
    auto opt_idx = Config::OptValInt::create(0);
    auto opt_bumps = Config::OptValStrVec::create();
    auto opt_clinworker = Config::OptValInt::create(8);
    auto opt_cliburst = Config::OptValInt::create(1000);
    auto opt_client_port = Config::OptValInt::create(-1);
    auto opt_max_cli_msg = Config::OptValInt::create(65536); // 64K by default

    config_bump.add_opt("idx", opt_idx, Config::SET_VAL, 'i', "specify the index in the replica list");
    config_bump.add_opt("replica", opt_bumps, Config::APPEND, 'a', "add an replica to the list");
    config_bump.add_opt("clinworker", opt_clinworker, Config::SET_VAL, 'M', "the number of threads for client network");
    config_bump.add_opt("cliburst", opt_cliburst, Config::SET_VAL, 'B', "");
    config_bump.add_opt("cport", opt_client_port, Config::SET_VAL, 'c', "specify the port listening for clients");
    config_bump.add_opt("max-cli-msg", opt_max_cli_msg, Config::SET_VAL, 'S', "the maximum client message size");

    config_bump.parse(argc, argv);
    auto idx = opt_idx->get();

    // Get the addr:port of this bump
    std::vector<std::tuple<std::string, std::string, std::string>> bumps;
    for (const auto &s: opt_bumps->get())
    {
        auto res = trim_all(split(s, ","));
        if (res.size() != 3)
            throw HotStuffError("invalid replica info");
        bumps.push_back(std::make_tuple(res[0], res[1], res[2]));
    }
    std::string binding_addr = std::get<0>(bumps[idx]);   // Bump #idx
    auto p = split_ip_port_cport(binding_addr);
    size_t tmp;
    auto client_port = opt_client_port->get();
    try {
        client_port = stoi(p.second, &tmp);
    } catch (std::invalid_argument &) {
        throw HotStuffError("client port not specified");
    }

    // Get the addr:port of the corresponding node
    Config config_node(config_node_file.c_str());
    auto opt_replicas = Config::OptValStrVec::create();
    config_node.add_opt("replica", opt_replicas, Config::APPEND, 'a', "add an replica to the list");
    config_node.parse(argc, argv);

    std::vector<NetAddr> nodes;
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
    for (const auto &p: raw)
    {
        auto _p = split_ip_port_cport(p);
        size_t _;
        printf("p.first: %s, p.second: %s\n", _p.first.c_str(), _p.second.c_str());
        nodes.push_back(NetAddr(NetAddr(_p.first).ip, htons(stoi(_p.second, &_))));
    }

    // Setup network with clients
    ClientNetwork<opcode_t>::Config clinet_config;
    clinet_config.max_msg_size(opt_max_cli_msg->get());
    clinet_config
        .burst_size(opt_cliburst->get())
        .nworker(opt_clinworker->get());

    EventContext ec;
    auto cs = new ClientSide(idx, ec, NetAddr("0.0.0.0", client_port), clinet_config);
    auto shutdown = [&](int) { cs->stop(); };
    salticidae::SigEvent ev_sigint(ec, shutdown);
    salticidae::SigEvent ev_sigterm(ec, shutdown);
    ev_sigint.add(SIGINT);
    ev_sigterm.add(SIGTERM);

    ec.dispatch();

    return 0;
}
