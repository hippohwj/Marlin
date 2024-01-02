
#include <thread>
#include <algorithm>
#include "gtest/gtest.h"
#include "db/ARDB.h"

using namespace std;
using namespace arboretum;

TEST(ConfigParseTest,ClusterMemberParseTest) {
  char* arg0="configs/dist_sample.cfg";
  char* arg1="configs/dist_sample.cfg";
  char* argv[] = {arg0, arg1};
  ARDB::LoadConfig(2, argv); 
  EXPECT_EQ(0, 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "ConfigParseTest.*";
  return RUN_ALL_TESTS();
}