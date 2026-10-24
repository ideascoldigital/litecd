// A top-level function (tlas) using an external variable (extVars).
function(count=1) [
  {
    apiVersion: 'v1',
    kind: 'ConfigMap',
    metadata: { name: std.extVar('prefix') + '-cm' },
    data: { count: std.toString(count), sum: std.toString(std.foldl(function(a, b) a + b, std.range(1, count), 0)) },
  },
]
