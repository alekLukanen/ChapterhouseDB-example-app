
GITHUB_NETRC = "/home/alek/.netrc"

default_registry('pi0:30000')
allow_k8s_contexts('default')

k8s_yaml(
  helm( 
    "./helm-deploy/chdb-ex",
    values=["./helm-deploy/chdb-ex/values.yaml"],
    namespace="default", 
  )
)

print(GITHUB_NETRC)

"""
docker_build(
  "pi0:30000/chdb-ex-app",
  ".",
  dockerfile="Dockerfile",
  build_args={"GITHUB_NETRC": read_file(GITHUB_NETRC)},
)
"""

