FROM gcr.io/cloudshell-images/cloudshell:latest

# Install dependencies for VS Code's Go extension
RUN go get -u \
  github.com/acroca/go-symbols \
  github.com/cweill/gotests/... \
  github.com/davidrjenni/reftools/cmd/fillstruct \
  github.com/fatih/gomodifytags \
  github.com/go-delve/delve/cmd/dlv \
  github.com/godoctor/godoctor \
  github.com/haya14busa/goplay/cmd/goplay \
  github.com/josharian/impl \
  github.com/mdempsky/gocode \
  github.com/ramya-rao-a/go-outline \
  github.com/rogpeppe/godef \
  github.com/stamblerre/gocode \
  github.com/uudashr/gopkgs/cmd/gopkgs \
  golang.org/x/lint/golint \
  golang.org/x/tools/cmd/gorename \
  golang.org/x/tools/cmd/guru

RUN gcloud config set project retail-common-services-249016
