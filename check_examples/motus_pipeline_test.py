import re
import subprocess
import os


def is_running(text):
    pattern = r"Job <(\d+)>"
    match = re.search(pattern, text)
    if match:
        job_id = match.group(1)
        command = f"bjobs -l {job_id}"
        try:
            output = subprocess.check_output(
                command, shell=True, text=True, stderr=subprocess.DEVNULL
            )
            lines = output.strip().split("\n")
            status_line = next(line for line in lines if line.startswith("Status"))
            status = status_line.split(":")[1].strip()
            return status in ["RUN", "PEND"]
        except subprocess.CalledProcessError:
            return False
    else:
        return False


def check(entry_name, launch_exitcode, launch_stdout, launch_stderr):
    expected_files = [
        f"/hps/nobackup/rdf/metagenomics/service-team/projects/motus_aws_2023/outputs/SRP110813/{entry_name}/cmsearch/{entry_name}_SSU.fasta",
        f"/hps/nobackup/rdf/metagenomics/service-team/projects/motus_aws_2023/outputs/SRP110813/{entry_name}/mOTUs/{entry_name}_merged.fastq.motus",
        f"/hps/nobackup/rdf/metagenomics/service-team/projects/motus_aws_2023/outputs/SRP110813/{entry_name}/qc/decontamination_output_report.txt",
    ]
    is_job_running = is_running(launch_stdout)

    if is_job_running:
        return is_job_running, launch_exitcode, launch_stdout, launch_stderr

    for expected_file in expected_files:
        if not os.path.exists(expected_file):
            return False, 1, "", f"Missing file: {expected_file}"
    return False, 0, "", ""
