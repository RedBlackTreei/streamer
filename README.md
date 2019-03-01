We have three nodes, they are all 8c16g.

Here is server configuration:

```xml
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:util="http://www.springframework.org/schema/util"
       xsi:schemaLocation="
        http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans.xsd
        http://www.springframework.org/schema/util
        http://www.springframework.org/schema/util/spring-util.xsd">
    <!--
                                   Alter configuration below as needed.
    -->
    <bean class="org.apache.ignite.configuration.IgniteConfiguration">
        <property name="discoverySpi">
            <bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">
            ...
            </bean>
        </property>

        <property name="gridLogger">
            <bean class="org.apache.ignite.logger.log4j2.Log4J2Logger">
            <constructor-arg type="java.lang.String" value="log4j2.xml"/>
            </bean>
        </property>

        <property name="communicationSpi">
            <bean class="org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi">
                <property name="localPort" value="47175"/>
                <property name="messageQueueLimit" value="1024"/>
            </bean>
        </property>

        <property name="dataStorageConfiguration">
            <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
                <property name="writeThrottlingEnabled" value="true"/>
                <property name="storagePath" value="/data/ignite/persistence"/>
                <property name="walPath" value="/ignite-wal/ignite/wal"/>
                <property name="walArchivePath" value="/ignite-wal/ignite/wal/archive"/>
                <property name="dataRegionConfigurations">
                    <list>
                        <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
                            <property name="name" value="1G_DataRegion"/>
                            <property name="persistenceEnabled" value="true"/>
                            <property name="metricsEnabled" value="true"/>
                            <property name="initialSize" value="#{512L * 1024 * 1024}"/>
                            <property name="maxSize" value="#{1L * 1024 * 1024 * 1024}"/>
                        </bean>

                        <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
                            <property name="name" value="5G_DataRegion"/>
                            <property name="persistenceEnabled" value="true"/>
                            <property name="initialSize" value="#{512L * 1024 * 1024}"/>
                            <property name="maxSize" value="#{5L * 1024 * 1024 * 1024}"/>
			</bean>
                    </list>
                </property>
            </bean>
        </property>
        <property name="includeEventTypes">
            <list>
                <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_STARTED"/>
                <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_FINISHED"/>
                <util:constant static-field="org.apache.ignite.events.EventType.EVT_TASK_FAILED"/>
            </list>
        </property>
    </bean>
</beans>
```

When loading a part of records, the performance decreased a lot. Here is the logs:


> **Finished, took = 17178ms, count = 23740000**
**Finished, took = 9061ms, count = 23750000**
**Finished, took = 15808ms, count = 23760000**
Finished, took = 950ms, count = 23770000
Finished, took = 11ms, count = 23780000
Finished, took = 12ms, count = 23790000
**Finished, took = 16365ms, count = 23800000**
Finished, took = 11ms, count = 23810000
Finished, took = 12ms, count = 23820000
Finished, took = 12ms, count = 23830000
Finished, took = 11ms, count = 23840000
Finished, took = 1343ms, count = 23850000
**Finished, took = 13617ms, count = 23860000**
Finished, took = 11ms, count = 23870000
Finished, took = 14988ms, count = 23880000
Finished, took = 11ms, count = 23890000
Finished, took = 7368ms, count = 23900000
Finished, took = 12ms, count = 23910000
Finished, took = 12ms, count = 23920000
Finished, took = 13ms, count = 23930000
Finished, took = 13ms, count = 23940000
Finished, took = 14ms, count = 23950000
Finished, took = 10249ms, count = 23960000


If the backup set to 0, it performed as normal.