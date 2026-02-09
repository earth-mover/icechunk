import { Repository, Storage } from "@earthmover/icechunk";
import type { S3Credentials, S3Options } from "@earthmover/icechunk";
import { randomUUID } from "node:crypto";

const BUCKET = "testbucket";

const credentials: S3Credentials = {
  type: "Static",
  field0: {
    accessKeyId: "minio123",
    secretAccessKey: "minio123",
  },
};

const options: S3Options = {
  region: "us-east-1",
  endpointUrl: "http://localhost:9000",
  allowHttp: true,
  forcePathStyle: true,
};

function storageForPrefix(prefix: string): Storage {
  return Storage.newS3(BUCKET, prefix, credentials, options);
}

async function create(): Promise<void> {
  const prefix = `icechunk-cli/${randomUUID()}`;
  const storage = storageForPrefix(prefix);
  const repo = await Repository.create(storage);

  const encoder = new TextEncoder();

  // First commit: root group
  const session1 = await repo.writableSession("main");
  await session1.store.set(
    "zarr.json",
    encoder.encode(JSON.stringify({ zarr_format: 3, node_type: "group" }))
  );
  const snap1 = await session1.commit("Initialize root group");

  // Second commit: add an array
  const session2 = await repo.writableSession("main");
  await session2.store.set(
    "temperatures/zarr.json",
    encoder.encode(
      JSON.stringify({
        zarr_format: 3,
        node_type: "array",
        shape: [100],
        data_type: "float32",
        chunk_grid: {
          name: "regular",
          configuration: { chunk_shape: [10] },
        },
        chunk_key_encoding: {
          name: "default",
          configuration: { separator: "/" },
        },
        fill_value: 0,
        codecs: [{ name: "bytes", configuration: { endian: "little" } }],
      })
    )
  );
  const snap2 = await session2.commit("Add temperatures array");

  // Third commit: add another group and array
  const session3 = await repo.writableSession("main");
  await session3.store.set(
    "measurements/zarr.json",
    encoder.encode(JSON.stringify({ zarr_format: 3, node_type: "group" }))
  );
  await session3.store.set(
    "measurements/pressure/zarr.json",
    encoder.encode(
      JSON.stringify({
        zarr_format: 3,
        node_type: "array",
        shape: [50, 50],
        data_type: "float64",
        chunk_grid: {
          name: "regular",
          configuration: { chunk_shape: [25, 25] },
        },
        chunk_key_encoding: {
          name: "default",
          configuration: { separator: "/" },
        },
        fill_value: 0,
        codecs: [{ name: "bytes", configuration: { endian: "little" } }],
      })
    )
  );
  const snap3 = await session3.commit("Add measurements group with pressure array");

  // Create a branch and tag
  await repo.createBranch("dev", snap3);
  await repo.createTag("v1.0", snap3);

  console.log(prefix);
}

async function listCommits(prefix: string): Promise<void> {
  const storage = storageForPrefix(prefix);
  const repo = await Repository.open(storage);
  const session = await repo.readonlySession({ branch: "main" });

  // Walk back through commits by opening sessions at each snapshot
  let snapshotId: string | null = session.snapshotId;
  const visited = new Set<string>();

  while (snapshotId && !visited.has(snapshotId)) {
    visited.add(snapshotId);
    const snap = await repo.readonlySession({ snapshotId });
    const keys = await snap.store.list();
    console.log(`commit ${snapshotId}`);
    console.log(`  keys: ${keys.join(", ") || "(empty)"}`);
    console.log();

    // There's no parent pointer exposed in the JS API,
    // so we list all commits we can find
    break;
  }

  // For a full rev-list, enumerate all branches and their snapshots
  const branches = await repo.listBranches();
  const seen = new Set<string>();

  for (const branch of branches) {
    const branchSession = await repo.readonlySession({ branch });
    const id = branchSession.snapshotId;
    if (!seen.has(id)) {
      seen.add(id);
      console.log(`* ${id.slice(0, 12)} (${branch})`);
    }
  }

  const tags = await repo.listTags();
  for (const tag of tags) {
    const tagSession = await repo.readonlySession({ tag });
    const id = tagSession.snapshotId;
    if (!seen.has(id)) {
      seen.add(id);
      console.log(`* ${id.slice(0, 12)} (tag: ${tag})`);
    } else {
      console.log(`  ${id.slice(0, 12)} also tagged: ${tag}`);
    }
  }
}

async function listBranches(prefix: string): Promise<void> {
  const storage = storageForPrefix(prefix);
  const repo = await Repository.open(storage);
  const branches = await repo.listBranches();

  for (const branch of branches) {
    const session = await repo.readonlySession({ branch });
    console.log(`${branch}\t${session.snapshotId.slice(0, 12)}`);
  }
}

async function listTags(prefix: string): Promise<void> {
  const storage = storageForPrefix(prefix);
  const repo = await Repository.open(storage);
  const tags = await repo.listTags();

  for (const tag of tags) {
    const session = await repo.readonlySession({ tag });
    console.log(`${tag}\t${session.snapshotId.slice(0, 12)}`);
  }
}

async function main(): Promise<void> {
  const [command, ...args] = process.argv.slice(2);

  switch (command) {
    case "create":
      await create();
      break;
    case "list-commits":
      if (!args[0]) {
        console.error("Usage: main.ts list-commits <prefix>");
        process.exit(1);
      }
      await listCommits(args[0]);
      break;
    case "list-branches":
      if (!args[0]) {
        console.error("Usage: main.ts list-branches <prefix>");
        process.exit(1);
      }
      await listBranches(args[0]);
      break;
    case "list-tags":
      if (!args[0]) {
        console.error("Usage: main.ts list-tags <prefix>");
        process.exit(1);
      }
      await listTags(args[0]);
      break;
    default:
      console.error("Usage: main.ts <create|list-commits|list-branches|list-tags> [prefix]");
      process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
