#!/usr/bin/env bash
# A temporary FSx for Lustre file system and EC2 clients for running fs-stress across hosts,
# as one CloudFormation stack (aws-lustre.yaml). See README.md, "FSx for Lustre on AWS".
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: aws-lustre.sh [--name NAME] COMMAND [ARGS...]
       aws-lustre.sh --help

A temporary Amazon FSx for Lustre file system (Persistent 2, SSD) and EC2 client instances
mounting it at /mnt/fsx, in a VPC of their own: one CloudFormation stack, from
aws-lustre.yaml next to this script. Uses the AWS CLI with the usual credentials and region
(AWS_PROFILE, AWS_REGION, ~/.aws/config). It costs money while it exists: run down when
you're done.

Options:
  --name NAME   the stack's name (default: fs-stress-fsx); also names the local cache
                directory (ssh key, the stack's account and region, results).

Commands:
  up [--yes] [--clients N] [--instance-type TYPE] [--az ZONE] [--ssh-cidr CIDR]
     [--throughput MBPS] [--storage-gib GIB]
      Create the stack and wait until every client has mounted the file system (20-40
      min), printing the stack's events as they happen. If anything fails, CloudFormation
      deletes everything it created. Asks before creating unless --yes.
      --clients N         client instances (default 5)
      --instance-type     client instance type (default c7i.large)
      --az ZONE           availability zone (default: the first that offers the type)
      --ssh-cidr CIDR     who may ssh to the clients (default: this machine's public IP)
      --throughput MBPS   Persistent 2 throughput per TiB: 125, 250, 500, 1000 (default 125)
      --storage-gib GIB   file system size: 1200 or a multiple of 2400 (default 1200)

  status           The stack's state, outputs and clients.
  events [-f]      The stack's events and what's still in progress; with -f, follow them
                   until the stack settles (e.g. from another terminal during up or down).
  hosts            The clients as ssh destinations, one per line, in client order.
  ssh N [CMD...]   ssh to client N as ec2-user (passwordless sudo).
  fs-stress [--results DIR] COMMAND [ARGS...]
      Run scripts/remote.sh COMMAND on all clients with the stack's ssh options. Results
      go to DIR (default: the cache directory, results/). The mount is /mnt/fsx.
      Run `aws-lustre.sh fs-stress --help` for remote.sh's commands.
  down             Delete the stack and wait until everything is gone.

Clients are numbered by private IP, so client 1 (which runs init and verify) stays the
same for the stack's lifetime.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

log() { echo "==> $*" >&2; }

here=$(cd "$(dirname "$0")" && pwd)
TEMPLATE=$here/aws-lustre.yaml
VM_USER=ec2-user
MOUNT=/mnt/fsx

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=5)

name="fs-stress-fsx"

set_name() {
  [[ $1 =~ ^[a-z][a-z0-9-]*$ ]] || die "name must start with a letter and be lowercase letters, digits and dashes: $1"
  name=$1
  CACHE_DIR=${XDG_CACHE_HOME:-$HOME/.cache}/$name
  KEY=$CACHE_DIR/id_ed25519
  # Where up recorded the stack's account, region and ID: "ACCOUNT REGION [STACK-ID]".
  WHERE=$CACHE_DIR/stack
}

region() {
  local r=${AWS_REGION:-${AWS_DEFAULT_REGION:-}}
  [[ -n $r ]] || r=$(aws configure get region || true)
  [[ -n $r ]] || die "no AWS region: set AWS_REGION or configure one for the profile"
  echo "$r"
}

account() { aws sts get-caller-identity --query Account --output text; }

# Every command but up works on the account and region up used, never on whatever the
# current profile points at, so a stack can't be missed by looking in the wrong place.
use_recorded() {
  [[ -f $WHERE ]] || die "no stack $name created from this machine ($WHERE is missing)"
  local acct reg _
  read -r acct reg _ <"$WHERE"
  [[ $(account) == "$acct" ]] || die "stack $name is in account $acct, but the current credentials are for $(account)"
  export AWS_REGION=$reg AWS_DEFAULT_REGION=$reg
}

cfn() { aws cloudformation "$@"; }

stack_status() {
  cfn describe-stacks --stack-name "$name" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo NONE
}

output() { # output <key>
  cfn describe-stacks --stack-name "$name" --output text \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue"
}

# The stack's ID when up recorded one, else its name: a stack deleted after a failed
# creation can only be found by ID.
stack_ref() {
  local id=""
  if [[ -f $WHERE ]]; then id=$(awk '{print $3}' "$WHERE"); fi
  echo "${id:-$name}"
}

# The stack's events as "id time resource status reason" lines (tab-separated), oldest first.
stack_events() {
  cfn describe-stack-events --stack-name "$(stack_ref)" --output text \
    --query 'reverse(StackEvents[].[EventId, Timestamp, LogicalResourceId, ResourceStatus, ResourceStatusReason])' \
    2>/dev/null || true
}

print_event() { # print_event <time> <resource> <status> <reason>
  local reason=$4
  [[ $reason != None ]] || reason=""
  printf '%s  %-22s %-20s %s\n' "$(date -d "$1" +%T 2>/dev/null || echo "${1:11:8}")" "$2" "$3" "$reason"
}

# The stack's resources still being created or deleted.
in_progress() {
  cfn describe-stack-resources --stack-name "$(stack_ref)" --output text \
    --query "StackResources[?ends_with(ResourceStatus, 'IN_PROGRESS')].LogicalResourceId" 2>/dev/null |
    tr '\t' ' ' || true
}

# Why the stack failed: its failed events, oldest first.
failures() {
  local id time res status reason
  while IFS=$'\t' read -r id time res status reason; do
    if [[ $status == *FAILED ]]; then print_event "$time" "$res" "$status" "$reason" >&2; fi
  done < <(stack_events)
}

# Prints the stack's events to stderr as they happen until the stack settles, and after 2
# minutes without one what it's still working on; then prints the final status (NONE:
# deleted). With skip-old, events from before the call aren't printed.
follow() { # follow [skip-old]
  local id time res state reason status start=$SECONDS quiet=0 new
  local -A seen=()
  if [[ ${1:-} == skip-old ]]; then
    while IFS=$'\t' read -r id _; do seen[$id]=1; done < <(stack_events)
  fi
  while true; do
    new=0
    while IFS=$'\t' read -r id time res state reason; do
      [[ -n $id && -z ${seen[$id]:-} ]] || continue
      seen[$id]=1
      new=1
      print_event "$time" "$res" "$state" "$reason" >&2
    done < <(stack_events)
    status=$(stack_status)
    [[ $status == *_IN_PROGRESS ]] || break
    if [[ $new -eq 1 ]]; then quiet=0; else quiet=$((quiet + 1)); fi
    if [[ $quiet -ge 8 ]]; then
      echo "$(date +%T)  ... $(((SECONDS - start) / 60)) min, still working on: $(in_progress)" >&2
      quiet=0
    fi
    sleep 15
  done
  echo "$status"
}

# Running clients as "private-ip public-ip" lines, by private IP.
clients() {
  local group
  group=$(output ClientGroup)
  [[ -n $group && $group != None ]] || die "stack $name has no clients (status $(stack_status))"
  aws ec2 describe-instances --output text \
    --filters "Name=tag:aws:autoscaling:groupName,Values=$group" Name=instance-state-name,Values=running \
    --query 'Reservations[].Instances[].[PrivateIpAddress, PublicIpAddress]' | sort -V
}

cmd_up() {
  local yes=0 n_clients=5 instance_type=c7i.large az="" ssh_cidr="" throughput=125 storage=1200
  while [[ $# -gt 0 ]]; do
    case $1 in
      --yes) yes=1 ;;
      --clients) n_clients=${2:?--clients needs a value}; shift ;;
      --instance-type) instance_type=${2:?--instance-type needs a value}; shift ;;
      --az) az=${2:?--az needs a value}; shift ;;
      --ssh-cidr) ssh_cidr=${2:?--ssh-cidr needs a value}; shift ;;
      --throughput) throughput=${2:?--throughput needs a value}; shift ;;
      --storage-gib) storage=${2:?--storage-gib needs a value}; shift ;;
      *) die "unknown option for up: $1" ;;
    esac
    shift
  done
  [[ $n_clients =~ ^[1-9][0-9]*$ ]] || die "--clients must be a positive number: $n_clients"
  [[ $storage == 1200 || ($storage =~ ^[1-9][0-9]*$ && $((storage % 2400)) -eq 0) ]] ||
    die "--storage-gib must be 1200 or a multiple of 2400: $storage"
  if [[ -f $WHERE ]]; then
    die "$WHERE exists: stack $name may still exist; run down first"
  fi
  local reg acct
  reg=$(region)
  acct=$(account)
  [[ $(stack_status) == NONE ]] || die "stack $name already exists in $reg"
  if [[ -z $ssh_cidr ]]; then
    ssh_cidr=$(curl -fsS --max-time 10 https://checkip.amazonaws.com | tr -d '[:space:]')/32
  fi
  if [[ -z $az ]]; then
    az=$(aws ec2 describe-instance-type-offerings --output text --location-type availability-zone \
      --filters "Name=instance-type,Values=$instance_type" --query 'InstanceTypeOfferings[].Location' |
      tr '\t' '\n' | sort | head -1)
    [[ -n $az ]] || die "no availability zone in $reg offers $instance_type"
  fi

  cat >&2 <<EOF
Stack $name in account $acct, region $reg, zone $az:
  FSx for Lustre 2.15, PERSISTENT_2 SSD, $storage GiB at $throughput MB/s/TiB, no backups
  $n_clients x $instance_type clients (Amazon Linux 2023), ssh from $ssh_cidr
This costs money until 'aws-lustre.sh down'.
EOF
  if [[ $yes -eq 0 ]]; then
    local answer
    read -rp "Create it? [y/N] " answer
    [[ $answer == [yY] ]] || die "cancelled"
  fi

  mkdir -p "$CACHE_DIR"
  [[ -f $KEY ]] || ssh-keygen -q -t ed25519 -N "" -C "$name" -f "$KEY"
  # Recorded before creating, so down finds the stack even if this script dies; the
  # stack's ID is added once known.
  echo "$acct $reg" >"$WHERE"
  log "creating stack $name (20-40 min, mostly the file system)"
  local id
  id=$(cfn create-stack --stack-name "$name" --template-body "file://$TEMPLATE" \
    --on-failure DELETE \
    --parameters \
    "ParameterKey=AvailabilityZone,ParameterValue=$az" \
    "ParameterKey=SshCidr,ParameterValue=$ssh_cidr" \
    "ParameterKey=PublicKey,ParameterValue=$(cat "$KEY.pub")" \
    "ParameterKey=ClientCount,ParameterValue=$n_clients" \
    "ParameterKey=InstanceType,ParameterValue=$instance_type" \
    "ParameterKey=StorageGiB,ParameterValue=$storage" \
    "ParameterKey=ThroughputPerTiB,ParameterValue=$throughput" \
    --tags "Key=fs-stress-lab,Value=$name" --query StackId --output text)
  echo "$acct $reg $id" >"$WHERE"
  wait_for CREATE_COMPLETE
  echo "Ready: $(output MountSource) on $n_clients clients at $MOUNT." >&2
  echo "Next: aws-lustre.sh fs-stress deploy BINARY; aws-lustre.sh fs-stress preflight $MOUNT" >&2
}

# Follows the stack until it settles; fails unless it settles in <want>.
wait_for() { # wait_for <want> [skip-old]
  local status
  status=$(follow "${2:-}")
  if [[ $status != "$1" ]]; then
    echo "stack $name is $status:" >&2
    failures
    die "stack $name didn't reach $1"
  fi
}

cmd_events() {
  use_recorded
  if [[ ${1:-} == -f ]]; then
    echo "stack $name: $(follow)"
    return
  fi
  local id time res state reason
  while IFS=$'\t' read -r id time res state reason; do
    print_event "$time" "$res" "$state" "$reason"
  done < <(stack_events)
  echo "stack $name: $(stack_status); in progress: $(in_progress)"
}

cmd_status() {
  use_recorded
  echo "stack $name in $AWS_REGION: $(stack_status)"
  [[ $(stack_status) == NONE ]] && return
  echo "file system $(output FileSystemId): $(output MountSource)"
  local k=0 private public
  while read -r private public; do
    k=$((k + 1))
    echo "client$k $private $public"
  done < <(clients)
}

cmd_hosts() {
  use_recorded
  local private public
  while read -r private public; do
    echo "$VM_USER@$public"
  done < <(clients)
}

cmd_ssh() {
  [[ $# -ge 1 && $1 =~ ^[1-9][0-9]*$ ]] || die "usage: aws-lustre.sh ssh N [CMD...]"
  use_recorded
  local public
  public=$(clients | awk -v k="$1" 'NR == k {print $2}')
  [[ -n $public ]] || die "no running client $1"
  shift
  ssh "${SSH_OPTS[@]}" -i "$KEY" "$VM_USER@$public" "$@"
}

cmd_fs_stress() {
  local results=$CACHE_DIR/results hosts
  if [[ ${1:-} == -h || ${1:-} == --help ]]; then exec "$here/../scripts/remote.sh" --help; fi
  if [[ ${1:-} == --results ]]; then
    results=${2:?--results needs a value}
    shift 2
  fi
  hosts=$(cmd_hosts | tr '\n' ' ')
  [[ -n $hosts ]] || die "stack $name has no running clients"
  mkdir -p "$results"
  exec "$here/../scripts/remote.sh" \
    --hosts "${hosts% }" \
    --results "$results" \
    --ssh-opts "-o BatchMode=yes -i $KEY ${SSH_OPTS[*]}" \
    "$@"
}

cmd_down() {
  use_recorded
  if [[ $(stack_status) != NONE ]]; then
    log "deleting stack $name in $AWS_REGION"
    cfn delete-stack --stack-name "$name"
    wait_for NONE skip-old
  fi
  rm -f "$WHERE" "$KEY" "$KEY.pub"
  log "stack $name is gone (results in $CACHE_DIR/results are kept)"
}

set_name "$name"
while [[ $# -gt 0 ]]; do
  case $1 in
    -h | --help) usage; exit 0 ;;
    --name) set_name "${2:?--name needs a value}"; shift 2 ;;
    *) break ;;
  esac
done
[[ $# -ge 1 ]] || { usage >&2; exit 1; }
command -v aws >/dev/null || die "the AWS CLI (aws) isn't installed"
command=$1
shift
case $command in
  up) cmd_up "$@" ;;
  status) cmd_status "$@" ;;
  events) cmd_events "$@" ;;
  hosts) cmd_hosts "$@" ;;
  ssh) cmd_ssh "$@" ;;
  fs-stress) cmd_fs_stress "$@" ;;
  down) cmd_down "$@" ;;
  *) die "unknown command: $command (see aws-lustre.sh --help)" ;;
esac
