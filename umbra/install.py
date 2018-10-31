"""
Supporting functions for installing as a systemd service.
"""

from .util import *
from .config import path_for_config
import configparser
import tempfile
import pwd
import grp
import subprocess

DRYRUN = False

logger = logging.getLogger(__name__)

def _install_file(path_src, path_dst, uid=-1, gid=-1, mode=None):
    """Copy a file with the given ownership and permissions."""
    if Path(path_dst).is_dir():
        path_dst = str(Path(path_dst) / Path(path_src).name)
    args = (path_src, path_dst)
    if Path(path_src).resolve() == Path(path_dst).resolve():
        logger.info("Installing %s to %s skipped, src and dst same" %  args)
        return
    if mode:
        args = (path_src, path_dst, uid, gid, oct(mode))
        logger.info("Installing %s to %s (uid %s, gid %s, mode %s)" % args)
    else:
        args = (path_src, path_dst, uid, gid)
        logger.info("Installing %s to %s (uid %s, gid %s)" % args)
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
    if Path(path_dst).exists():
        os.unlink(path_dst)
    os.link(tmp_dst, path_dst)
    # 5: remove tmp
    os.close(fd)
    os.unlink(tmp_dst)

def _install_dir(path, uid=-1, gid=-1, mode=None):
    """Create a directory with the given ownership and permissions."""
    if mode:
        args = (path, uid, gid, oct(mode))
        logger.info("Setting up directory %s (uid %s, gid %s, mode %s)" % args)
    else:
        args = (path, uid, gid)
        logger.info("Setting up directory %s (uid %s, gid %s)" % args)
    if DRYRUN:
        return
    os.makedirs(path, exist_ok=True)
    os.chown(path, uid, gid)
    if not mode is None:
        os.chmod(path, mode)

def _setup_systemd_exec(path_exec):
    """Find or create executable script for service launch."""
    # First off, do we even have systemd?
    result = subprocess.run(["systemctl", "--version"], stdout=subprocess.PIPE,
            universal_newlines=True)
    if result.returncode:
        msg = '"systemctl" command failed.  Is systemd not available?'
        logger.warn(msg)
    else:
        version = result.stdout.split('\n')[0]
        msg = 'Version line from systemctl: "%s"' % version
        logger.info(msg)
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
        wrapper += 'exec umbra "$@"\n'
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
    logger.info("configuring systemd service")

    # Check executable and create wrapper script if needed
    path_exec = _setup_systemd_exec(path_exec)

    homedir = pwd.getpwuid(uid).pw_dir
    user = pwd.getpwuid(uid).pw_name
    group = grp.getgrgid(gid).gr_name
    cmd = "%s --action process -v" % str(path_exec)
    # If the service is stopped, it will wait for up to 30 minutes after
    # sending the terminate signal before sending the kill signal.
    stop_timeout = 30*60

    logger.info("detecting user details")
    logger.info("username: %s" % user)
    logger.info("groupname: %s" % group)
    logger.info("homedir: %s" % homedir)

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
            "ExecStart": cmd,
            "TimeoutStopSec": stop_timeout,
            "StandardOutput": "syslog",
            "StandardError": "syslog",
            "SyslogIdentifier": "umbra"
            }
    service["Install"] = {
            "WantedBy": "multi-user.target"
            }

    logger.info("writing systemd configuration to %s" % service_path)
    if not DRYRUN:
        mkparent(service_path)
        with open(service_path, "w") as f:
            service.write(f, space_around_delimiters=False)
    # TODO need to daemon-reload or something if there's a new service file?

def _setup_paths(config, uid, gid):
    logger.info("creating directory paths")
    # Detect necessary directory paths from live configuration
    created = set()
    # Configure log path to be writable for syslog
    # https://serverfault.com/a/527088
    log_path = Path("/var/log/umbra")
    log_gid = grp.getgrnam("syslog").gr_gid
    _install_dir(log_path, uid, log_gid, 0o775)
    created.add(log_path)
    # First set up the basic directory paths.
    r = Path(config["paths"]["root"])
    p = config["paths"]
    path_entries = [
      (p["runs"], -1, -1, None),
      (p["experiments"], -1, -1, None),
      (p["status"], uid, gid, 0o755),
      (p["processed"], uid, gid, 0o755),
      (p["packaged"], uid, gid, 0o755)
      ]
    for path, uid, gid, mode in path_entries:
        if not Path(path).is_absolute():
            path = r / path
        _install_dir(path, uid, gid, mode)
        created.add(path)
    # Set up the parents of these file entries with appropriate permissions.
    others = [
              ("save_report", "path", 0o755),
              ("box", "credentials_path", 0o700),
              ("mailer", "credentials_path", 0o700)
            ]
    for section, key, mode in others:
        dir_path = Path(config[section][key]).parent
        if not dir_path in created:
            _install_dir(dir_path, uid, gid, mode)
        created.add(dir_path)

def _setup_config(config_path):
    logger.info("Installing config file")
    if not config_path or not Path(config_path).exists():
        logger.info("no existing config file; using package default.")
        config_path = path_for_config()
    _install_file(config_path, "/etc/umbra.yml", mode=0o644)

def _setup_rsyslog():
    path_conf = Path(__file__).parent / "data" / "10-umbra.conf"
    path_dir = Path("/etc/rsyslog.d")
    if path_dir.exists():
        _install_file(path_conf, path_dir)
        # TODO logger.info("Restarting rsyslog")

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
    _setup_rsyslog()
    _setup_systemd("/etc/systemd/system/umbra.service", path_exec, uid, gid)
