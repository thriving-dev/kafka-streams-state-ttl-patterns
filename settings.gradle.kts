pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name="kafka-streams-state-ttl-patterns"

include("common-avros")
include("common-datagen")
include("pattern1-iterate-all-delete")
include("pattern2-ktable-iterate-all-send-tombstones")
include("pattern3-lrucache-compact-delete")
include("pattern4-rocksdb-ttl-compact-delete")
include("pattern5-iterate-idx-range-scan")
include("pattern6-iterate-idx-prefix-scan")
include("pattern7-data-expiry-job-consumer")
include("pattern8-lazy-value-deserializer")
