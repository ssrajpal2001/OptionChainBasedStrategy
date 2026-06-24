module.exports = {
  apps: [{
    name: 'algo',
    script: 'run_system.py',
    interpreter: 'python3',
    args: '--mode live --ui --port 5000 --index NIFTY,SENSEX --strategies sell_straddle,trap_scanner',
    cwd: '/home/ec2-user/OptionChainBasedStrategy',
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
  }]
};
