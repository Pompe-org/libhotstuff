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
using salticidae::Config;

int main(int argc, char **argv) {
    Config config(argv[1]);
    auto opt_idx = Config::OptValInt::create(0);
    config.add_opt("idx", opt_idx, Config::SET_VAL, 'i', "specify the index in the replica list");
    config.parse(argc, argv);
    auto idx = opt_idx->get();

    printf("This is the speedbump#%d\n", idx);
    
    return 0;
}
