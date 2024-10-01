
# GITHUB_NETRC = "/home/alek/.netrc"

default_registry('pi0:30000')
allow_k8s_contexts('default')

k8s_yaml(
  helm( 
    "./helm-deploy/chdb-ex",
    values=["./helm-deploy/chdb-ex/values.yaml"],
    namespace="default", 
  )
)


# build the worker #################
local_resource(
  'go-worker-build',
  'GOARCH=arm64 GOOS=linux go build -o ./bin/worker ./cmd/worker/main.go',
  deps=['./cmd/worker/main.go'],
)
docker_build(
  "pi0:30000/chdb-ex-worker",
  ".",
  dockerfile="Dockerfile.worker",
  only=["./bin"],
  # build_args={"GITHUB_NETRC": read_file(GITHUB_NETRC)},
)
###################################

# build the producer ##############
local_resource(
  'go-tester-build',
  'CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ GOARCH=arm64 GOOS=linux go build -o ./bin/tester ./cmd/tester/main.go',
  deps=['./cmd/tester/main.go'],
)
docker_build(
  "pi0:30000/chdb-ex-tester",
  ".",
  dockerfile="Dockerfile.tester",
  only=["./bin"],
  # build_args={"GITHUB_NETRC": read_file(GITHUB_NETRC)},
)
###################################

