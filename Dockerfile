# syntax=docker/dockerfile:1
FROM python:3.13-slim AS build

WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src

# Version comes from the tag — .git is not in the build context,
# so hatch-vcs reads SETUPTOOLS_SCM_PRETEND_VERSION instead.
ARG VERSION=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION}
RUN pip install --no-cache-dir build && python -m build --wheel


FROM python:3.13-slim
LABEL org.opencontainers.image.source="https://github.com/pingedbrain/microburst" \
      org.opencontainers.image.description="AWS-protocol-aware fault injection proxy" \
      org.opencontainers.image.licenses="MIT"
COPY --from=build /src/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -f /tmp/*.whl
EXPOSE 9999
ENTRYPOINT ["microburst"]
CMD ["--help"]
