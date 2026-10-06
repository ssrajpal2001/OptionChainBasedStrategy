module.exports = {
  apps: [{
    name: 'terminus',
    script: 'run_system.py',
    interpreter: 'python3',
    args: '--mode live --ui --index NIFTY --strategies v4_cascade --port 8080',
    cwd: __dirname,
    env: {
      V4CASCADE_USE_POOL_ENGINE: '1',
      V4CASCADE_TRACKING_OFFSETS: '100,200,300',
      LOG_LEVEL: 'INFO',
    },
    max_memory_restart: '700M',
    watch: false,
    autorestart: true,
    restart_delay: 5000,
    instances: 1,
    exec_mode: 'fork',
    log_date_format: 'YYYY-MM-DD HH:mm:ss',
    out_file: 'logs/pm2-out.log',
    error_file: 'logs/pm2-err.log',
    merge_logs: true,
  }]
};
