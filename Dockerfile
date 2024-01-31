FROM dev.exactspace.co/python3.8-base-es2:r1
COPY *.py /tmp/
COPY main /tmp/
RUN chmod +x /tmp/*
WORKDIR /tmp
ENTRYPOINT ["./main"]
