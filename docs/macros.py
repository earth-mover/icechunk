import os
from pathlib import Path
import logging

def define_env(env):
    # TODO: is there a better way of including these files and dirs? Symlinking seems error prone...
    # Potentially use: https://github.com/backstage/mkdocs-monorepo-plugin
    def symlink_external_dirs():
        """
        Creates symbolic links from external directories to the docs_dir.
        """
        try:
            # Resolve paths for docs and monorepo root
            docs_dir = Path('./docs').resolve()
            monorepo_root = docs_dir.parent.parent

            # Symlinked paths
            external_sources = {
                monorepo_root / 'icechunk-python' / 'notebooks' : docs_dir / 'icechunk-python' / 'notebooks',
                monorepo_root / 'icechunk-python' / 'examples' : docs_dir / 'icechunk-python' / 'examples',
                monorepo_root / 'spec' / 'icechunk-spec.md' : docs_dir / 'spec.md',
            }

            for src, target in external_sources.items():
                if not src.exists():
                    logging.error(f"Source directory does not exist: {src}")
                    raise FileNotFoundError(f"Source directory does not exist: {src}")

                # Ensure parent directory exists
                target.parent.mkdir(parents=True, exist_ok=True)

                # Remove existing symlink or directory if it exists
                if target.is_symlink() or target.exists():
                    if target.is_dir() and not target.is_symlink():
                        logging.error(f"Directory {target} already exists and is not a symlink.")
                        raise Exception(f"Directory {target} already exists and is not a symlink.")
                    target.unlink()
                    logging.info(f"Removed existing symlink or directory at: {target}")

                # Create symbolic link
                os.symlink(src, target)
                logging.info(f"Created symlink: {target} -> {src}")

        except Exception as e:
            logging.error(f"Error creating symlinks: {e}")
            raise e

    # Execute the symlink creation
    symlink_external_dirs()