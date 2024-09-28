
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
  only=["./bin/worker"],
  # build_args={"GITHUB_NETRC": read_file(GITHUB_NETRC)},
)
###################################

# build the producer ##############
local_resource(
  'go-producer-build',
  'GOARCH=arm64 GOOS=linux go build -o ./bin/producer ./cmd/producer/main.go',
  deps=['./cmd/producer/main.go'],
)
docker_build(
  "pi0:30000/chdb-ex-producer",
  ".",
  dockerfile="Dockerfile.producer",
  only=["./bin/producer"],
  # build_args={"GITHUB_NETRC": read_file(GITHUB_NETRC)},
)
###################################

