#!/usr/bin/env bash
#
# Does an S3 gateway drop user-metadata header names that contain "_"?
#
# nginx sets `underscores_in_headers off` by default and discards such a header.
# When that header is also in the SigV4 SignedHeaders list, the gateway sees a
# signed header that is not in the request, and MinIO-derived gateways answer
# "AccessDenied: There were headers present in the request which were not signed".
# The message points at the wrong thing: nothing extra arrived, something signed
# went missing. Icechunk therefore keeps every object-metadata key alphanumeric.
#
# The probe needs no credentials, because MinIO-derived code validates
# SignedHeaders before it looks up the access key.
#
# Usage:
#   scripts/gateway-metadata-header-probe.sh [endpoint] [bucket]
#   REPS=20 scripts/gateway-metadata-header-probe.sh https://t3.storage.dev
#   IPS="1.2.3.4 5.6.7.8" scripts/gateway-metadata-header-probe.sh
#
# DNS hands out one slice of an anycast fleet at a time, so a single run can miss
# the affected nodes. Pass IPS to pin the addresses you want to compare.
#
# Reads "." as "header survived" and "R" as "header dropped".

set -uo pipefail

ENDPOINT=${1:-https://t3.storage.dev}
BUCKET=${2:-icechunk-nonexistent-probe-bucket}
REPS=${REPS:-10}

HOST=${ENDPOINT#https://}
HOST=${HOST#http://}
HOST=${HOST%%/*}

DATE=$(date -u +%Y%m%dT%H%M%SZ)
SCOPE_DATE=${DATE%%T*}
EMPTY_SHA=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
FAKE_SIG=0000000000000000000000000000000000000000000000000000000000000000

probe_once() {
  local ip=$1 header=$2 resolve=()
  [ -n "$ip" ] && resolve=(--resolve "${HOST}:443:${ip}")
  curl -s --max-time 20 "${resolve[@]}" -X PUT "${ENDPOINT}/${BUCKET}/probe.txt" \
    -H "x-amz-date: ${DATE}" \
    -H "x-amz-content-sha256: ${EMPTY_SHA}" \
    -H "${header}: probe" \
    -H "Authorization: AWS4-HMAC-SHA256 Credential=probe/${SCOPE_DATE}/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;${header}, Signature=${FAKE_SIG}" \
    --data-binary "" \
  | grep -oE "<Code>[^<]*" | head -1 | cut -d'>' -f2
}

server_of() {
  local ip=$1 resolve=()
  [ -n "$ip" ] && resolve=(--resolve "${HOST}:443:${ip}")
  curl -s --max-time 15 "${resolve[@]}" -o /dev/null -D - "${ENDPOINT}/" \
  | grep -i '^server:' | tr -d '\r' | cut -d' ' -f2- | head -1
}

# One anycast name can front a mixed fleet, so probe each address on its own.
ips=${IPS:-}
if [ -z "$ips" ] && command -v dig >/dev/null 2>&1; then
  ips=$(dig +short "$HOST" A | grep -E '^[0-9.]+$' | sort -u)
fi

printf '%-16s %-14s %-26s %s\n' NODE SERVER HEADER RESULT
for ip in ${ips:-""}; do
  server=$(server_of "$ip")
  for header in x-amz-meta-probe_key x-amz-meta-probekey; do
    row=""
    for _ in $(seq 1 "$REPS"); do
      case "$(probe_once "$ip" "$header")" in
        AccessDenied) row="${row} R" ;;
        InvalidAccessKeyId | SignatureDoesNotMatch) row="${row} ." ;;
        *) row="${row} ?" ;;
      esac
    done
    printf '%-16s %-14s %-26s%s\n' "${ip:-default}" "${server:-?}" "$header" "$row"
  done
done

cat <<'LEGEND'

  .  header survived, the request reached signature or key validation
  R  header dropped, gateway answered "headers present ... which were not signed"
  ?  unexpected response, inspect by hand

A gateway that prints R for the underscore row and . for the plain row mangles
metadata header names. Icechunk avoids it by keeping keys alphanumeric.
LEGEND
