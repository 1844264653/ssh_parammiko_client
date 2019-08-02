import pymysql
import atexit

from .ssh_clinet import SSH


class MySQLClient(object):
    CACHE_CLIENT = {}

    def __init__(self, host, database=None, mysql_port=3306, ssh_port=22,
                 ssh_user="root", ssh_password="adminsangfornetwork", refresh=True):
        """
        initialize client to use 'self.conn' and 'self.cursor' for operating mysql
        :param host: ip of the host
        :param mysql_port: port of the mysql, default is 3306
        :param ssh_port: port of the ssh, default is 22
        :param ssh_user: user of the ssh, default is "root"
        :param ssh_password: password of the ssh, default is "admin"
        """
        self.host = host
        if self.host in self.CACHE_CLIENT and database in self.CACHE_CLIENT[self.host] \
                and not refresh:
            self.ssh_client = self.CACHE_CLIENT[host][database]["ssh_client"]
            self.conn = self.CACHE_CLIENT[host][database]["conn"]
            self.cursor = self.CACHE_CLIENT[host][database]["cursor"]
            # self.conn.ping()

        else:
            if "sangfornetwork" not in ssh_password:
                ssh_password = ssh_password + "sangfornetwork"

            self.ssh_client = SSH(host=host, user=ssh_user, password=ssh_password, port=ssh_port)
            self.add_mysql_port_rule(port=mysql_port)
            self.allow_access()
            config = {
                "host": host,
                "port": mysql_port,
                "user": "root",
                "database": database,
                "charset": "utf8"
            }
            self.conn = pymysql.connect(**config)
            self.conn.autocommit(1)
            self.cursor = self.conn.cursor()
            temp = {
                database: {
                    "ssh_client": self.ssh_client,
                    "conn": self.conn,
                    "cursor": self.cursor,
                }
            }
            if self.host in self.CACHE_CLIENT:
                self.CACHE_CLIENT[self.host].update(temp)
            else:
                self.CACHE_CLIENT[self.host] = temp

    def allow_access(self):
        script = "mysql -e \" use mysql; update user set host = '%' where user = 'root';" \
                 " FLUSH PRIVILEGES\""
        self.ssh_client.execute(script)

    def add_mysql_port_rule(self, port=3306):
        query_rule = "iptables -L INPUT --line-number | grep mysql | awk '{print $1}'"
        query_results = self.ssh_client.execute(query_rule).strip().split("\n")
        if query_results == [""]:
            self.ssh_client.execute("iptables -A INPUT -p tcp -w 5 --dport %s -j ACCEPT" % port)
            # LOG.info("Add the rules for releasing mysql port")

    @classmethod
    def delete_mysql_port_rule(cls, port=3306):
        for host, temp in cls.CACHE_CLIENT.items():
            for database, info in temp.items():
                ssh = info.get("ssh_client")
                query_rule = "iptables -L INPUT --line-number | grep mysql | awk '{print $1}'"
                query_results = ssh.execute(query_rule).strip().split("\n")
                if query_results == [""]:
                    # LOG.info("There is not exist mysql rule, so don't delete")
                    return
                for index in range(len(query_results)):
                    result = ssh.execute(query_rule).strip().split("\n")[0]
                    if result:
                        ssh.execute("iptables -w 5 -D INPUT %s" % result)
                        # LOG.info("Remove the rules for releasing the mysql port")

                conn = info.get("conn")
                cursor = info.get("cursor")
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()

    def execute(self, sql, ret_number="all", size=None, args=None):
        self.cursor.execute(sql, args)
        if ret_number == "all":
            result = self.cursor.fetchall()
        # elif ret