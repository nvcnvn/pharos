REPO=vllm-project/vllm
MODEL=qwen2.5-0.5b
READY=/health
image() { # Docker Hub has CPU images from v0.16.0; older ones are on vLLM's ECR repo (amd64)
  if [ "$(printf '%s\n' "$VERSION" v0.16.0 | sort -V | head -1)" = v0.16.0 ]; then
    echo "vllm/vllm-openai-cpu:$VERSION"
  else
    echo "public.ecr.aws/q9t5s3a7/vllm-cpu-release-repo:$VERSION"
  fi
}
needs() { # v0.10.2, v0.12.0 and v0.16.0 exit with SIGILL without it (docs/SUPPORT.md)
  if [ "$(printf '%s\n' "$VERSION" v0.16.0 | sort -V | tail -1)" = v0.16.0 ]; then echo avx512f; fi
}
