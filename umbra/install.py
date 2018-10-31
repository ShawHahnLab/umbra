"""
Supporting functions for installing as a systemd service.
"""

from .util import *
from .config import path_for_config
import configparser
import tempfile
import pwd
import grp

DRYRUN = False

logger = logging.getLogger(__name__)

def _install_file(path_src, path_dst, uid=-1, gid=-1, mode=None):
    """Copy a file with the given ownership and permissions."""
    dirs = (path_src, path_dst)
    if Path(path_src).resolve() == Path(path_dst).resolve():
        logger.info("Installing %s to %s skipped, src and dst same" %  dirs)
        return
    logger.info("Installing %s to %s" % dirs)
    if DRYRUN:
        return
    # Installing via a tempfile to make sure permissions and ownership are
    # correct once file "arrives" at destination.  This is a little paranoid
    # but should prevent an interrupted install from ever leaving files around
    # with the wrong ownership or permissions.
    # 1: create as temporary file
    parent = Path(path_dst).parent
    fd, tmp_dst = tempfile.mkstemp(dir=parent)
    with open(path_src) as f_in, open(tmp_dst, "w") as f_out:
        f_out.write(f_in.read())
    # 2: set perms
    current_umask = os.umask(0)
    os.umask(current_umask)
    os.chmod(tmp_dst, 0x1ff ^ current_umask)
    if not mode is None:
        os.chmod(tmp_dst, mode)
    # 3: set ownership
    os.chown(tmp_dst, uid, gid)
    # 4: os.link()
    os.link(tmp_dst, path_dst)
    # 5: remove tmp
    os.close(fd)
    os.unlink(tmp_dst)

def _install_dir(path, uid=-1, gid=-1, mode=None):
    """Create a directory with the given ownership and permissions."""
    logger.info("Setting up directory %s" % path)
    if DRYRUN:
        return
    os.makedirs(path, exist_ok=True)
    os.chown(path, uid, gid)
    if not mode is None:
        os.chmod(path, mode)

def _setup_systemd_exec(path_exec):
    """Find or create executable script for service launch."""
    # Get current executable path and permissions
    # Watch out!  In theory this could be the __main__.py file and not the
    # exeutable entry point, depending on how this was launched.
    # First make sure executable is executable.
    info = os.stat(path_exec)
    if not info.st_mode & 0o111:
        msg = "Executable path not actually executable;"
        msg += " service may not work.  Be sure to run via the correct script."
        logger.warn(msg)
    # Next, are we in a conda environment?  Do we need a wrapper to activate that?
    # Check if the executable lives within the conda prefix path, if there is one.
    in_conda = False
    conda_prefix = os.getenv("CONDA_PREFIX")
    if conda_prefix:
        logger.info("Anaconda detected: %s" % conda_prefix)
        conda_parts = Path(conda_prefix).resolve().parts
        exec_parts = path_exec.resolve().parts
        if conda_parts == exec_parts[0:len(conda_parts)]:
            logger.info("Executable is within Anaconda env: %s" % conda_prefix)
            in_conda = True
    # Set up wrapper
    if in_conda:
        conda_env = os.getenv("CONDA_DEFAULT_ENV")
        conda_exe = os.getenv("CONDA_EXE")
        activate = Path(conda_exe).parent / "activate"
        wrapper = '#!/usr/bin/env bash\n'
        wrapper += 'source "%s" "%s"\n' % (activate, conda_env)
        wrapper += 'umbra "$@"\n'
        path_wrapper = "/var/lib/umbra/umbra-wrapper.sh"
        logger.info("Writing wrapper script: %s" % path_wrapper)
        if not DRYRUN:
            mkparent(path_wrapper)
            with open(path_wrapper, "w") as f:
                f.write(wrapper)
            os.chmod(path_wrapper, 0o755)
        return(path_wrapper)
    return(path_exec)

# Adapted from:
# https://stackoverflow.com/a/30189540
def _setup_systemd(service_path, path_exec, uid, gid):
    logger.info("systemd: configuring service")

    # Check executable and create wrapper script if needed
    path_exec = _setup_systemd_exec(path_exec)

    homedir = pwd.getpwuid(uid).pw_dir
    user = pwd.getpwuid(uid).pw_name
    group = grp.getgrgid(gid).gr_name

    service = configparser.ConfigParser()
    # By default it transforms to all lowercase, but we don't want that for
    # systemd.
    service.optionxform = str
    service["Unit"] = {
            "Description": "Illumina sequencing data processing",
            "After": "syslog.target"
            }
    service["Service"] = {
            "Type": "simple",
            "User": user, 
            "Group": group,
            "WorkingDirectory": homedir,
            "ExecStart": path_exec,
            "StandardOutput": "syslog",
            "StandardError": "syslog"
            }
    service["Install"] = {
            "WantedBy": "multi-user.target"
            }

    logger.info("systemd: writing configuration to %s" % service_path)
    if not DRYRUN:
        mkparent(service_path)
        with open(service_path, "w") as f:
            service.write(f, space_around_delimiters=False)
    # TODO need to daemon-reload or something if there's a new service file?

def _setup_paths(config, uid, gid):
    logger.info("creating directory paths")
    # Detect necessary directory paths from live configuration
    # First set up the basic directory paths.
    paths = []
    r = Path(config["paths"]["root"])
    p = config["paths"]
    for key, val in config["paths"].items():
        if key == "root":
            continue
        if Path(val).is_absolute():
            paths.append(val)
        else:
            paths.append(r / val)
    for path in paths:
        _install_dir(path, uid, gid, 0o755)
    # Set up the parents of these file entries with appropriate permissions.
    others = [
              ("save_report", "path", 0o755),
              ("box", "credentials_path", 0o700),
              ("mailer", "credentials_path", 0o700)
            ]
    for entry in others:
        dir_path = Path(config[entry[0]][entry[1]]).parent
        _install_dir(dir_path, uid, gid, entry[2])

def _setup_config(config_path):
    logger.info("Installing config file")
    if not config_path or not Path(config_path).exists():
        logger.info("no existing config file; using package default.")
        config_path = path_for_config()
    _install_file(config_path, "/etc/umbra.yml", mode=0o644)

def install(config, config_path):
    """Set up filesystem paths and a systemd service.
    
    config: the currently loaded config dict.
    config_path: the configuration file to copy to /etc/.  If not specified or
    nonexistent the package default will be used."""
    # Get current executable path and permissions
    path_exec = Path(sys.argv[0])
    info = os.stat(path_exec)
    uid = info.st_uid
    gid = info.st_gid
    _setup_config(config_path)
    _setup_paths(config, uid, gid)
    _setup_systemd("/etc/systemd/system/umbra.service", path_exec, uid, gid)
