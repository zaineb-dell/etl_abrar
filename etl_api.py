"""
etl_api.py — API Flask pour contrôler le pipeline ETL depuis Laravel.
Lance avec : py etl_api.py
Port : 5000
"""

from flask import Flask, jsonify, request
import subprocess
import threading
import os
import sys

app = Flask(__name__)

ETL_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable

# Etat des jobs
jobs_status = {
    "MMG": {"detail": "Stopped", "agg": "Stopped", "dw": "Stopped"},
    "OCC": {"detail": "Stopped", "agg": "Stopped", "dw": "Stopped"},
}

logs = []


def run_script(script, args=[], job_key=None, step_key=None):
    """Lance un script Python en arrière-plan."""
    if job_key and step_key:
        jobs_status[job_key][step_key] = "Running"
    try:
        result = subprocess.run(
            [PYTHON, os.path.join(ETL_DIR, script)] + args,
            capture_output=True, text=True, cwd=ETL_DIR
        )
        log = result.stdout + result.stderr
        logs.append({"script": script, "log": log})
        if job_key and step_key:
            if result.returncode == 0:
                jobs_status[job_key][step_key] = "Done"
            else:
                jobs_status[job_key][step_key] = "Error"
        return result.returncode == 0, log
    except Exception as e:
        if job_key and step_key:
            jobs_status[job_key][step_key] = "Error"
        return False, str(e)


@app.route("/api/etl/status", methods=["GET"])
def get_status():
    """Retourne l'état de tous les jobs."""
    return jsonify(jobs_status)


@app.route("/api/etl/run/mmg", methods=["POST"])
def run_mmg():
    """Lance le pipeline ETL complet MMG."""
    def task():
        run_script("main.py", ["--type", "MMG"], "MMG", "detail")
    threading.Thread(target=task).start()
    return jsonify({"message": "Pipeline MMG démarré"})


@app.route("/api/etl/run/occ", methods=["POST"])
def run_occ():
    """Lance le pipeline ETL complet OCC."""
    def task():
        run_script("main.py", ["--type", "OCC"], "OCC", "detail")
    threading.Thread(target=task).start()
    return jsonify({"message": "Pipeline OCC démarré"})


@app.route("/api/etl/run/dw", methods=["POST"])
def run_dw():
    """Lance uniquement la partie Data Warehouse."""
    def task():
        run_script("run_dw.py", [], "MMG", "dw")
    threading.Thread(target=task).start()
    return jsonify({"message": "Chargement DW démarré"})


@app.route("/api/etl/run/all", methods=["POST"])
def run_all():
    """Lance le pipeline complet MMG + OCC + DW."""
    def task():
        run_script("main.py", [], "MMG", "detail")
        run_script("run_dw.py", [], "MMG", "dw")
    threading.Thread(target=task).start()
    return jsonify({"message": "Pipeline complet démarré"})


@app.route("/api/etl/logs", methods=["GET"])
def get_logs():
    """Retourne les derniers logs ETL."""
    return jsonify(logs[-50:] if len(logs) > 50 else logs)


@app.route("/api/etl/reset", methods=["POST"])
def reset_status():
    """Remet tous les jobs à Stopped."""
    for flux in jobs_status:
        for step in jobs_status[flux]:
            jobs_status[flux][step] = "Stopped"
    return jsonify({"message": "Status réinitialisé"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)