# Here we will use the busybox as the base image
FROM busybox:latest
RUN echo "hello this is busybox"
CMD ["uname", "-m"]
