import os
import subprocess
from pathlib import Path


def move_X509() -> str:
    """move x509 proxy file from /tmp to /afs/private. Returns the afs path"""
    try:
        x509_localpath = (
            [
                line
                for line in os.popen("voms-proxy-info").read().split("\n")
                if line.startswith("path")
            ][0]
            .split(":")[-1]
            .strip()
        )
    except Exception as err:
        raise RuntimeError(
            "x509 proxy could not be parsed, try creating it condowith 'voms-proxy-init --voms cms'"
        ) from err
    x509_path = f"{Path.home()}/private/{x509_localpath.split('/')[-1]}"
    subprocess.run(["cp", x509_localpath, x509_path])
    return x509_path


def submit_condor(args: dict) -> None:
    """build condor and executable files. Submit condor job"""
    main_dir = Path.cwd()
    condor_dir = Path(main_dir / "condor")
    
    jobpath = f"{args['processor']}/{args['year']}"
    if args["processor"] == "tag_eff":
        jobpath = f'{jobpath}/{args["tagger"]}/{args["flavor"]}/{args["wp"]}'
        
    # create logs directory
    log_dir = Path(condor_dir / "logs" / jobpath)
    if not log_dir.exists():
        log_dir.mkdir(parents=True)
    
    # set jobname
    jobname = f'{args["processor"]}_'
    jobname += args["fileset"].split("/")[-1].replace(".json", "")
        
    # creal local condor submit file
    local_condor_path = Path(condor_dir / jobpath)
    if not local_condor_path.exists():
        local_condor_path.mkdir(parents=True)
    local_condor = f"{local_condor_path}/{jobname}.sub"
    
    # make condor file
    condor_template_file = open(f"{condor_dir}/submit.sub")
    condor_file = open(local_condor, "w")
    for line in condor_template_file:
        line = line.replace("DIRECTORY", str(condor_dir))
        line = line.replace("JOBNAME", jobname)
        line = line.replace("JOBPATH", jobpath)
        line = line.replace("JOBFLAVOR", f'"longlunch"')
        condor_file.write(line)
    condor_file.close()
    condor_template_file.close()

    # make executable file
    x509_path = move_X509() 
    sh_template_file = open(f"{condor_dir}/submit.sh")
    local_sh = f"{local_condor_path}/{jobname}.sh"
    sh_file = open(local_sh, "w")
    for line in sh_template_file:
        line = line.replace("MAINDIRECTORY", str(main_dir))
        line = line.replace("COMMAND", args["cmd"])
        line = line.replace("X509PATH", x509_path)
        sh_file.write(line)
    sh_file.close()
    sh_template_file.close()

    # submit jobs
    print(f"submitting {jobname}")
    subprocess.run(["condor_submit", local_condor])