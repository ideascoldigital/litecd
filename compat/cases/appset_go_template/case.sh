desc="ApplicationSet goTemplate: fields, a pipeline (upper), printf, if/else, missingkey=error"
phase=3
timeout=300
t_case() {
    repo_init
    app_apply appset.yaml
    wait_app go-p Synced Healthy
    wait_app go-q Synced Healthy
    snapshot --appset gotemplate generated
}
