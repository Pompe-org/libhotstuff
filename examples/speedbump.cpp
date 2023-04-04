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

class Speedbump {
    int idx, cnt;
    EventContext ec;
    EventContext req_ec;
    EventContext resp_ec;
    std::thread req_thread, resp_thread;

    // For client side
    ClientNetwork<opcode_t> cn;
    std::unordered_map<const uint256_t, NetAddr> pending_resp;

    // For server side
    using Net = salticidae::MsgNetwork<opcode_t>;
    Net mn;
    Net::conn_t node_conn;

    // For debugging
    int num_forwarded, num_backwarded;

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
        pending_resp[cmd_hash] = addr;
        //printf("Bump #%d forwarding %s\n", idx, std::string(*cmd).c_str());
        // Forward client request to one node
        MsgReqCmd msg_forward(*cmd);
        mn.send_msg(msg_forward, node_conn);
        num_forwarded++;
    }

    void client_resp_handler(MsgRespCmd &&msg, const conn_t &) {
        auto &fin = msg.fin;
        const uint256_t &cmd_hash = fin.cmd_hash;
        //printf("Bump #%d returns %s\n", idx, get_hex(cmd_hash).c_str());
        // Return nodes response back to client
        NetAddr addr = pending_resp[cmd_hash];
        cn.send_msg(MsgRespCmd(std::move(fin)), addr);
        num_backwarded++;
    }

public:
    Speedbump(int idx,
              const EventContext &ec,
              NetAddr clisten_addr,
              NetAddr node_addr,
              const ClientNetwork<opcode_t>::Config &clinet_config):
        ec(ec),
        idx(idx),
        mn(resp_ec, Net::Config()),
        cn(req_ec, clinet_config) {

        // Connect to node
        mn.reg_handler(salticidae::generic_bind(&Speedbump::client_resp_handler, this, _1, _2));
        mn.start();
        node_conn = mn.connect_sync(node_addr);

        // Connect to client
        cn.reg_handler(salticidae::generic_bind(&Speedbump::client_req_handler, this, _1, _2));
        cn.start();
        cn.listen(clisten_addr);

        req_thread = std::thread([this]() { req_ec.dispatch(); });
        resp_thread = std::thread([this]() { resp_ec.dispatch(); });
        //while(1);
    }

    void stop() {
        printf("[DEBUG] Bump #%d forward=%d, backward=%d\n", idx, num_forwarded, num_backwarded);
        req_ec.stop();
        resp_ec.stop();
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

    NetAddr node;
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
    {
        auto _p = split_ip_port_cport(raw[idx]);
        size_t _;
        //printf("Bump#%d connects to %s, %s\n", idx, _p.first.c_str(), _p.second.c_str());
        node = NetAddr(NetAddr(_p.first).ip, htons(stoi(_p.second, &_)));
    }

    // Setup network with clients
    ClientNetwork<opcode_t>::Config clinet_config;
    clinet_config.max_msg_size(opt_max_cli_msg->get());
    clinet_config
        .burst_size(opt_cliburst->get())
        .nworker(opt_clinworker->get());

    EventContext ec;
    auto cs = new Speedbump(idx, ec, NetAddr("0.0.0.0", client_port), node, clinet_config);
    auto shutdown = [&](int) { cs->stop(); };
    salticidae::SigEvent ev_sigint(ec, shutdown);
    salticidae::SigEvent ev_sigterm(ec, shutdown);
    ev_sigint.add(SIGINT);
    ev_sigterm.add(SIGTERM);

    ec.dispatch();

    return 0;
}
