# Prerequisites
1) `gem install tmuxinator`
2) `brew install tmux`
3) `cp ez-enki.yml ~/.tmuxinator/ez-enki.yml`
4) `cp ez-nabu.yml ~/.tmuxinator/ez-nabu.yml`

# Starting cluster
1) <new terminal session>
2) `tmuxinator start ez-enki`
3) <new terminal session>
4) `tmuxinator start ez-nabu`

# Stopping cluster
1) tmux kill-session -t ez-enki
2) tmux kill-session -t ez-nabu
