import os
import sys

def try_exec_cmd(cmd):
    print("execute: %s" % cmd)
    try:
        # subprocess.run(cmd, shell=True, check=True)
        os.system(cmd)
    except Exception as e:
        print("ERROR executing: {}".format(cmd))
        print(e)


if __name__ == "__main__":
  user="wjhu"
  homedir="/users/{}/Arboretum-Distributed".format(user)

  compute_nodes_hosts=["node-0", "node-1", "node-2", "node-3"]
  # copy code updates to every nodes
  for node in range(len(compute_nodes_hosts)):
      if node != 0:
          try_exec_cmd("rsync -av --exclude 'outputs' --exclude 'dist_outputs_collection' --exclude 'build' --exclude 'compile_out' --delete {} {}@{}:{}".format(homedir+"/", user, compute_nodes_hosts[node], homedir))
  print("[INFO] code sync finished!")
