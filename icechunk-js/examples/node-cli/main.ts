import { Repository, Storage } from "@earthmover/icechunk";
import { resolve } from "node:path";

function storageForPath(path: string): Promise<Storage> {
  return Storage.newLocalFilesystem(resolve(path));
}

async function create(path: string): Promise<void> {
  const storage = await storageForPath(path);
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

  console.log(`Created repo at ${resolve(path)}`);
}

async function listBranches(path: string): Promise<void> {
  const storage = await storageForPath(path);
  const repo = await Repository.open(storage);
  const branches = await repo.listBranches();

  for (const branch of branches) {
    const session = await repo.readonlySession({ branch });
    console.log(`${branch}\t${session.snapshotId.slice(0, 12)}`);
  }
}

async function listTags(path: string): Promise<void> {
  const storage = await storageForPath(path);
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
      if (!args[0]) {
        console.error("Usage: main.ts create <path>");
        process.exit(1);
      }
      await create(args[0]);
      break;
    case "list-branches":
      if (!args[0]) {
        console.error("Usage: main.ts list-branches <path>");
        process.exit(1);
      }
      await listBranches(args[0]);
      break;
    case "list-tags":
      if (!args[0]) {
        console.error("Usage: main.ts list-tags <path>");
        process.exit(1);
      }
      await listTags(args[0]);
      break;
    default:
      console.error("Usage: main.ts <create|list-branches|list-tags> <path>");
      process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
