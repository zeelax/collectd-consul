import consul
import collectd

CONSUL_CONFIG = {
    'Host':           'localhost',
    'Port':           8500,
    'Verbose':        False,
}

def get_consul_conn():
    return consul.Consul(
        host=CONSUL_CONFIG['Host'],
        port=CONSUL_CONFIG['Port']
    )

def configure_callback(conf):
    global CONSUL_CONFIG
    for node in conf.children:
        if node.key in CONSUL_CONFIG:
            CONSUL_CONFIG[node.key] = node.values[0]

    CONSUL_CONFIG['Port']    = int(CONSUL_CONFIG['Port'])
    CONSUL_CONFIG['Verbose'] = bool(CONSUL_CONFIG['Verbose'])

def dispatch_value(prefix, key, value, type, type_instance=None):
    if not type_instance:
        type_instance = key

    log_verbose('Sending value: %s/%s=%s' % (prefix, type_instance, value))
    if not value:
        return
    try:
        value = int(value)
    except ValueError:
        value = float(value)

    val               = collectd.Values(plugin='consul', plugin_instance=prefix)
    val.type          = type
    val.type_instance = type_instance
    val.values        = [value]
    val.dispatch()

def log_verbose(msg):
    if CONSUL_CONFIG['Verbose'] == False:
        return
    collectd.info('consul plugin: %s' % msg)

def read_callback():
    conn = get_consul_conn()

    service_check_result = {}
    service_list = conn.catalog.services()[1]

    for service in service_list.keys():
        service_check_result[service] = {}
        service_check_result[service]['passing'] = 0
        service_check_result[service]['warning'] = 0
        service_check_result[service]['critical'] = 0
        service_check_result[service]['total'] = 0
        
        service_check_list = conn.health.checks(service)[1]
        for service_check in service_check_list:
            check_name = service_check['Name']
            check_status = service_check['Status']

            service_check_result[service][check_status] += 1
            service_check_result[service]['total'] += 1

        for service in service_check_result.keys():
            dispatch_value(service, 'checks_passing', service_check_result[service]['passing'], 'gauge')
            dispatch_value(service, 'checks_warning', service_check_result[service]['warning'], 'gauge')
            dispatch_value(service, 'checks_critical', service_check_result[service]['critical'], 'gauge')
            dispatch_value(service, 'checks_warning', service_check_result[service]['warning'], 'gauge')
            if service_check_result[service]['passing'] == service_check_result[service]['total']:
                dispatch_value(service, 'isok', 1, 'gauge')
            else:
                dispatch_value(service, 'isok', 0, 'gauge')

# register callbacks
collectd.register_config(configure_callback)
collectd.register_read(read_callback)
