# The CPU image is built for Intel Xeon (amd64 only).
REPO=sgl-project/sglang
MODEL=qwen2.5-0.5b
READY=/health
image() { echo "lmsysorg/sglang:$VERSION-xeon"; }
