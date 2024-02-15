package fleet

const RoleWorker = "worker"
const RoleLeader = "leader"
const RoleLoadBalancer = "loadbalancer"

const ScriptEndpointVersion = "/ccms/fleet/setup.sh"

// ValidRoles will be used to check if incoming request has the validate role set
var ValidRoles = []string{RoleLeader, RoleWorker, RoleLoadBalancer}
