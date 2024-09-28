
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

local_resource(
  'go-build',
  'GOARCH=arm64 GOOS=linux go build -o ./bin/app ./cmd/app/main.go',
  deps=['./cmd/app/main.go'],
)

docker_build(
  "pi0:30000/chdb-ex-worker",
  ".",
  dockerfile="Dockerfile.local",
  only=["./bin"],
  # build_args={"GITHUB_NETRC": read_file(GITHUB_NETRC)},
)

