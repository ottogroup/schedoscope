import time
from cm_api.api_client import ApiResource

# configuration
JDK_CONFIG = {'java_home' : '/usr/java/jdk1.8.0_181'}
PARCEL_REPO = 'http://archive.cloudera.com/cdh5/parcels/5.14.0/,http://archive.cloudera.com/spark2/parcels/2.2.0.cloudera2/'
PARCELS = [
    { 'name' : "CDH", 'version' : "5.14.0-1.cdh5.14.0.p0.24" },
    { 'name' : "SPARK2", 'version' : "2.2.0.cloudera2-1.cdh5.12.0.p0.232957" }
]


class ParcelInstaller:
    def __init__(self, name, version):
        self.name = name
        self.version = version

    def install(self, cluster):
        parcel = cluster.get_parcel(self.name, self.version)

        # download the parcel
        print "Starting to download parcel %s %s" % (self.name, self.version)
        cmd = parcel.start_download()
        if cmd.success != True:
            print "Download parcel %s %s has been failed!" % (self.name, self.version)
            exit(0)

        while parcel.stage != 'DOWNLOADED':
            time.sleep(5)
            parcel = cluster.get_parcel(self.name, self.version)
            if parcel.state.errors:
                raise Exception(str(parcel.state.errors))
            completed = (float(parcel.state.progress) / float(parcel.state.totalProgress)) * 100
            print "download progress: %.2f%%" % round(completed, 2)

        print "Parcel %s %s has been downloaded." % (self.name, self.version)

        # distribute the parcel
        print "Starting to distribute parcel %s %s" % (self.name, self.version)
        cmd = parcel.start_distribution()
        if cmd.success != True:
            print "Distribution of parcel %s %s has been failed!" % (self.name, self.version)
            exit(0)

        while parcel.stage != 'DISTRIBUTED':
            time.sleep(5)
            parcel = cluster.get_parcel(self.name, self.version)
            if parcel.state.errors:
                raise Exception(str(parcel.state.errors))
            completed = (float(parcel.state.progress) / float(parcel.state.totalProgress)) * 100
            print "distribution progress: %.2f%%" % round(completed, 2)

        print "Parcel %s %s has been distributed" % (self.name, self.version)

        print "Activating parcel %s %s" % (self.name, self.version)
        cmd = parcel.activate()
        if cmd.success != True:
            print "Parcel %s %s activation failed!" % (self.name, self.version)
            exit(0)

        while parcel.stage != "ACTIVATED":
            parcel = cluster.get_parcel(self.name, self.version)

        print "Parcel %s %s has been activated." % (self.name, self.version)


def main():
    resource = ApiResource("localhost", 7180, "cloudera", "cloudera", version=19)
    cluster = resource.get_cluster("Cloudera Quickstart")

    cm_manager = resource.get_cloudera_manager()
    cm_manager.update_config({'REMOTE_PARCEL_REPO_URLS': PARCEL_REPO})
    cm_manager.update_all_hosts_config(JDK_CONFIG)
    time.sleep(5)

    for parcel in PARCELS:
        ParcelInstaller(parcel['name'], parcel['version']).install(cluster)

    print "Restarting cluster"
    cluster.stop().wait()
    cluster.start().wait()
    print "Done restarting cluster"


if __name__ == "__main__":
    main()
