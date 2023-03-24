FROM python



WORKDIR /worker
git clone git@xxx.com  xxx

VOLUME /worker

RUN pip install virtualenv &&  \
    virtualenv create xxx_repo ./  \
    && pip install -r requirements.txt  && spider worker task_run ./


ENTRYPOINT ["spider worker"]