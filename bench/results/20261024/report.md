| | Argo CD | litecd |
|---|---|---|
| Memory, no apps (working set) | 194.2 MiB | 2.2 MiB |
| … anonymous (heap) of that | 169.4 MiB | 1.2 MiB |
| Memory, 20 apps synced, idle (working set) | 310.2 MiB | 6.9 MiB |
| … anonymous (heap) of that | 225.0 MiB | 4.1 MiB |
| Memory, peak during the burst | 325.5 MiB | 9.4 MiB |
| CPU idle, no apps (% of one CPU) | 0.24 % | 0.004 % |
| CPU idle, 20 apps | 0.382 % | 0.147 % |
| Install → ready | 28.7 s | 0.6 s |
| App created → Synced + Healthy (median, range) | 0.46 s (0.46–1.1) | 0.46 s (0.45–2.86) |
| Commit + refresh → Synced on it | 0.39 s (0.39–0.39) | 0.68 s (0.39–0.68) |
| Deployment deleted → healed, Healthy | 0.43 s (0.43–0.44) | 0.77 s (0.77–0.77) |
| 19 apps at once → all Synced + Healthy | 5.72 s | 3.28 s |

- Argo CD, 20 apps idle, per container: argocd-application/application-controller 161.4 MiB, argocd-applicationset-controller/applicationset-controller 45.7 MiB, argocd-redis/redis 12.3 MiB, argocd-repo-server/repo-server 46.2 MiB, argocd-server/server 44.6 MiB
- litecd, 20 apps idle, per container: litecd/litecd 6.9 MiB
