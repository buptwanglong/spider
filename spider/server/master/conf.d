root@xxx:/etc/supervisor/conf.d# cat C9019_app.conf
[program:C9019_app]  # 这段要注意，改成你自己的
command=/opt/C9019/webapps/start.sh start   # 启动脚本，有什么参数都给添加上，这个脚本是自己写的
directory=/opt/C9019/webapps   # 在哪个目录下启动程序
autostart=true
exitcodes=0
autorestart=true
startretries=3
exitcodes=0,2
#stopsignal=TERM
#stopasgroup=true
stopwaitsecs=2
user=root
export JAVA_HOME=/opt/jdk1.8.0_121
export JRE_HOME=/opt/jdk1.8.0_121/jre
export PATH=${JAVA_HOME}/bin:${JRE_HOME}/bin:$PATH
stdout_logfile=/var/log/supervisord/C9019_app_stdout.log
stderr_logfile=/var/log/supervisord/C9019_app_stderr.log

