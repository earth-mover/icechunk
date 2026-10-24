#!/usr/bin/env bash
# Slurm controller (slurmctld) of cluster "fs-stress", whose one partition "lab" has every
# client, and the login node to run srun and sbatch on. Clients run configless: their slurmd
# fetches slurm.conf and cgroup.conf from here. Limits are those of a typical site: jobs get
# cores and memory (cons_tres, CR_Core_Memory), enforced with cgroups. No accounting
# (slurmdbd), so sacct doesn't work.
# shellcheck source=common.sh
source /tmp/fs-stress/common.sh

phase_install() {
  base_setup
  ensure_kernel "$KERNEL" kernel kernel-core kernel-modules-core
  install_slurm slurm-slurmctld-ohpc
}

# Writes slurm.conf, with one node per entry of $SLURM_NODES, and cgroup.conf, and starts or
# reconfigures slurmctld, which also sends them to the nodes. slurmctld doesn't start without
# nodes.
write_config() {
  local entry name cpus mem nodes="" defmem=""
  for entry in $SLURM_NODES; do
    IFS=: read -r name cpus mem <<<"$entry"
    nodes+="NodeName=$name CPUs=$cpus RealMemory=$mem State=UNKNOWN"$'\n'
    # The smallest node's memory per core, so that a job can have all of a node's cores.
    if [[ -z $defmem || $((mem / cpus)) -lt $defmem ]]; then defmem=$((mem / cpus)); fi
  done
  install -d /etc/slurm
  cat >/etc/slurm/slurm.conf <<EOS
ClusterName=fs-stress
SlurmctldHost=slurm-srv
SlurmctldParameters=enable_configless
SlurmUser=slurm
StateSaveLocation=/var/spool/slurmctld
SlurmdSpoolDir=/var/spool/slurmd
AuthType=auth/munge
ProctrackType=proctrack/cgroup
TaskPlugin=task/cgroup,task/affinity
SelectType=select/cons_tres
SelectTypeParameters=CR_Core_Memory
DefMemPerCPU=${defmem:-1024}
JobAcctGatherType=jobacct_gather/cgroup
KillWait=30
ReturnToService=2
${nodes}PartitionName=lab Nodes=ALL Default=YES MaxTime=INFINITE State=UP
EOS
  cat >/etc/slurm/cgroup.conf <<EOS
ConstrainCores=yes
ConstrainRAMSpace=yes
EOS
  if [[ -z $nodes ]]; then
    log "no clients yet: slurmctld starts once provision adds some"
    systemctl disable --now slurmctld
  elif systemctl is-active -q slurmctld; then
    scontrol reconfigure
  else
    systemctl enable --now slurmctld
  fi
}

phase_configure() {
  if [[ ! -f /etc/munge/munge.key ]]; then
    (umask 077 && dd if=/dev/urandom of=/etc/munge/munge.key bs=1024 count=1 status=none)
  fi
  chown munge:munge /etc/munge/munge.key
  chmod 400 /etc/munge/munge.key
  systemctl enable --now munge
  # The slurm user comes from slurm-ohpc, with the same uid on every node.
  install -d -o slurm -g slurm /var/spool/slurmctld
  write_config
  [[ -z $SLURM_NODES ]] || sinfo
}

# Bring slurm.conf's nodes up to date, before new clients start slurmd.
phase_nodes() {
  [[ -f /etc/munge/munge.key ]] || { log "not configured yet: run lab.sh provision slurm-srv"; exit 1; }
  write_config
  sinfo -N
}

"phase_$1"
