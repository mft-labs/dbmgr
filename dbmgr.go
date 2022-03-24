package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"strings"
	"time"
)
const (
	INSTALL_CONFIG="insert into amf_settings(setting_id, config_type, config, user_audit_info) values($1,$2,$3,$4)"
)

type DbMgr struct {
	Con *sql.DB
}

func (db *DbMgr) ReadFile(fpath string) ([]byte, error) {
	contents, err := ioutil.ReadFile(fpath)
	return contents,err
}

func (db *DbMgr) InitDb(url string) *sql.DB {
	con, err := sql.Open("postgres", url)
	if err != nil {
		fmt.Printf("Error occurred while connecting to Database:%v\n",err)
		return nil
	}
	return con
}

func (db *DbMgr) WriteFile(fpath string, contents []byte) ( error) {
	err := ioutil.WriteFile(fpath,contents,0644)
	return err
}

func (db *DbMgr) Connect2Db(url string) (error) {
	db.Con = db.InitDb(url)
	if db.Con == nil {
		return fmt.Errorf("Error occurred while connecting to Database")
	}
	return nil
}
func (db *DbMgr) CreateSchema(url, schemaFile string) (err error) {
	fmt.Printf("Running process for create schema with %s\n",schemaFile)
	for {
		err = db.Connect2Db(url)
		if err!=nil {
			fmt.Printf("Error occurred while connecting to DB:%v\nRetrying ...\n",err)
			time.Sleep(time.Second*15)
		}
		break
	}

	fmt.Printf("Connected to database and reading file:%s\n",schemaFile)
	contents,err := db.ReadFile(schemaFile)
	if err!=nil {
		fmt.Printf("Error occurred while reading file :%s\n%v\n",schemaFile,err)
		return
	}
	schema := string(contents)

	tablesList := make([]string,0)
	tablesCount := 0
	for _, statement := range strings.Split(schema, ";") {
		statement = strings.TrimSpace(statement)
		if strings.Contains(statement,"CREATE TABLE ") {
			arr := strings.Split(statement,"(")
			if len(arr)>0 {
				tablename := strings.ToLower(strings.TrimSpace(arr[0][len("CREATE TABLE "):]))
				tablesList = append(tablesList,tablename)
				tablesCount += 1
			}
		}
	}

	validationQuery:=fmt.Sprintf(`SELECT
									table_schema || '.' || table_name
								FROM
									information_schema.tables
								WHERE
									table_type = 'BASE TABLE'
								AND
									table_schema NOT IN ('pg_catalog', 'information_schema')`)
	verifyTables  := make(map[string]bool)
	for _, tablename := range tablesList {
		verifyTables[strings.TrimSpace(tablename)] = true
	}

	rows, err := db.Con.Query(validationQuery)
	if err!= nil {
		text  := fmt.Sprintf("%v",err)
		fmt.Printf("Database verification failed. Reason:%s\n",text)
	} else {
		defer rows.Close()

		existingTablesCount := 0
		matchedTables := make(map[string]bool)
		for rows.Next() {
			tableName := ""
			rows.Scan(&tableName)
			tableName = strings.TrimSpace(tableName)
			arr := strings.Split(tableName, ".")
			if len(arr) > 1 {
				tableName = arr[1]
			}
			match := "Not Matched"
			if _, ok := verifyTables[tableName]; ok {
				match = "Matched"
			}
			text := fmt.Sprintf("TABLE:%s (%s)", tableName, match)
			fmt.Printf("info:%v\n", text)
			if match == "Matched" {
				existingTablesCount += 1
				matchedTables[tableName] = true
			}
		}
		if tablesCount == existingTablesCount {
			fmt.Printf("All the required tables are available in the database\n")
			db.WriteFile("/apps/amf/amfdata/db-status",[]byte("Ready"))
			return nil
		}
	}
	fmt.Printf("Installing schema\n")
	tablesList = make([]string,0)
	tablesCount = 0
	for _, statement := range strings.Split(schema, ";") {
		//fmt.Printf("Statement: %v\n",statement)
		statement = strings.TrimSpace(statement)
		if strings.Contains(statement,"CREATE TABLE ") {
			arr := strings.Split(statement,"(")
			if len(arr)>0 {
				tablename := strings.ToLower(strings.TrimSpace(arr[0][len("CREATE TABLE "):]))
				tablesList = append(tablesList,tablename)
				tablesCount += 1
			}
		}
		_, err := db.Con.Exec(statement)
		if err != nil {
			//return fmt.Errorf( "Cant create statement :%s\n%v\n", statement,err)
			fmt.Printf("Skipping existing create statement. Reason:%v\n",err)
		}
	}
	validationQuery=fmt.Sprintf(`SELECT
									table_schema || '.' || table_name
								FROM
									information_schema.tables
								WHERE
									table_type = 'BASE TABLE'
								AND
									table_schema NOT IN ('pg_catalog', 'information_schema')`)
	//contents2, err := util.ReadFile(BASEPATH+"/amf/validations/table_validations.txt")
	//tablesList := string(contents2)
	verifyTables  = make(map[string]bool)
	//for _, tablename := range strings.Split(tablesList,"\n") {
	for _, tablename := range tablesList {
		verifyTables[strings.TrimSpace(tablename)] = true
	}

	rows, err = db.Con.Query(validationQuery)
	if err!= nil {
		text  := fmt.Sprintf("%v",err)
		return fmt.Errorf("Failed to get tables list, reason:%s",text)
	} else {
		defer rows.Close()

		existingTablesCount := 0
		matchedTables := make(map[string]bool)
		for rows.Next() {
			tableName := ""
			rows.Scan(&tableName)
			tableName = strings.TrimSpace(tableName)
			arr := strings.Split(tableName,".")
			if len(arr)>1 {
				tableName=arr[1]
			}
			match := "Not Matched"
			if _, ok:=verifyTables[tableName]; ok {
				match = "Matched"
			}
			text := fmt.Sprintf("TABLE:%s (%s)",tableName,match)
			fmt.Printf("info:%v\n",text)
			if match == "Matched" {
				existingTablesCount += 1
				matchedTables[tableName] = true
			}
		}
		if tablesCount == existingTablesCount {
			fmt.Printf("All the required tables are available in the database\n")
		} else {
			text := fmt.Sprintf("Database contains %d tables out of %d tables",existingTablesCount,tablesCount)
			fmt.Println(text)
			fmt.Println("info","The following tables are missed")
			for tableName,_ := range verifyTables {
				if _, ok:=matchedTables[tableName]; ok {
					text := fmt.Sprintf("Table found:%s",tableName)
					fmt.Printf("info%s\n",text)
				} else {
					text := fmt.Sprintf("Table missing:%s",tableName)
					fmt.Printf("info:%s\n",text)
				}
			}
			return fmt.Errorf("Failed to create schema, all the required tables are not created")
		}
	}
	db.WriteFile("/apps/amf/amfdata/db-status",[]byte("Ready"))
	return nil
}
func (db *DbMgr) InitUiSettings(dburl string) (err error){
	jsonStr2 := fmt.Sprintf(`{
  "APP_BUTTON_BACKGROUND_COLOR": "#5897ce",
  "APP_FAVICON_TYPE": "image/png",
  "APP_FAVICON_URL": "/resources/img/adi-favicon.png",
  "APP_LOGO_STYLES": "width:203px;margin-left:10px;margin-top:10px;",
  "APP_LOGO_URL": "/resources/img/agiledatainc-200X50.png",
  "APP_NAVBAR_HOVER_COLOR": "#5897ce",
  "CDReceiveDisable": "True",
  "CERTFILE": "certs/server.crt",
  "COMMUNITY_NAME": "MFTLABS_COMM",
  "COPYRIGHT_TEXT": "2022 Agile Data Inc, All rights are reserved.",
  "DEFAULT_PAGE_SIZE": "23",
  "DEFAULT_SCREEN": "Home",
  "DISABLE_SFTP_GET": "false",
  "EXTERNAL_PROXY_PASSWORD": "9oxFHmAtYsrLvOJY2jPWo8WHDMU5GtSFyGvKLQAqyWE",
  "EXTERNAL_PROXY_URL": "http://localhost:58443",
  "EXTERNAL_PROXY_USERNAME": "admin",
  "EnableGlobalMailbox": "True",
  "EnableVenafi": "Yes",
  "Environments": "Prod,Non-Prod",
  "FOOTER_BACKGROUND_COLOR": "navy",
  "FOOTER_FONT_WEIGHT": "600",
  "FOOTER_TEXT_COLOR": "#737690",
  "HEADER_BACKGROUND_COLOR": "navy",
  "HEADER_TEXT_COLOR": "#FFF",
  "INTERNAL_PROXY_PASSWORD": "9oxFHmAtYsrLvOJY2jPWo8WHDMU5GtSFyGvKLQAqyWE",
  "INTERNAL_PROXY_URL": "http://localhost:58443",
  "INTERNAL_PROXY_USERNAME": "admin",
  "KAFKA_HOST": "",
  "KAFKA_PARTITION": "",
  "KAFKA_PORT": "",
  "KAFKA_TOPIC": "",
  "KEYFILE": "certs/server.key",
  "KEYS_PATH": "/apps/amf/amfdata/keys",
  "KNOWNHOSTKEY_PARAM": "DC",
  "MADownloadButton": "True",
  "MQ_QUEUE_NAME": "amf_wf_registration_queue",
  "MessageActivityPageSize": "50",
  "ONBOARD_COMM_CREATE_CD_PROFILE": "CREATE_CD_PROFILE",
  "ONBOARD_COMM_CREATE_SFTP_PROFILE": "CREATE_SFTP_PROFILE",
  "ONBOARD_COMM_DELETE_CD_PROFILE": "DELETE_CD_PROFILE",
  "ONBOARD_COMM_DELETE_SFTP_PROFILE": "DELETE_SFTP_PROFILE",
  "ONBOARD_COMM_UPDATE_CD_PROFILE": "UPDATE_CD_PROFILE",
  "ONBOARD_COMM_UPDATE_SFTP_PROFILE": "UPDATE_SFTP_PROFILE",
  "ONBOARD_USER_GM_DELETE_MESSAGE_TYPE": "DELETE_GM_USER",
  "ONBOARD_USER_GM_MESSAGE_TYPE": "CREATE_GM_USER",
  "ONBOARD_USER_GM_UPDATE_MESSAGE_TYPE": "UPDATE_GM_USER",
  "ONBOARD_USER_RECEIVER": "AMF_USER",
  "ONBOARD_USER_SENDER": "AMF_USER",
  "ONBOARD_USER_TM_DELETE_MESSAGE_TYPE": "DELETE_TM_USER",
  "ONBOARD_USER_TM_MESSAGE_TYPE": "CREATE_TM_USER",
  "ONBOARD_USER_TM_UPDATE_MESSAGE_TYPE": "UPDATE_TM_USER",
  "SADownloadButton": "True",
  "SFG_API_BASE_URL": "http://localhost:40074",
  "SFG_API_BASE_URL_LIST": "http://localhost:40074",
  "SFG_API_PASSWORD": "password",
  "SFG_API_USERNAME": "amf_api_user",
  "SFG_WORKFLOW_API": "http://localhost:40074/B2BAPIs/svc/workflows/?_include=name&_range=0-999&fieldList=names&searchFor=AMF",
  "SFTP_OUTBOUND_PRIVATE_KEYID": "RwcEIjeoXCgmmencRSpkA5jc",
  "STORAGE_ROOT": "/apps/amf/amfdata",
  "ShowImportNavIcon": "true",
  "TIMEZONE": "Asia/Calcutta",
  "TLSV1": "TLS_RSA_WITH_AES_256_CBC_SHA",
  "TLSV11": "TLS_RSA_WITH_AES_256_CBC_SHA",
  "TLSV12": "TLS_RSA_WITH_AES_256_CBC_SHA256",
  "UFA_DOWNLOAD_URL": "",
  "UFA_VERSIONS": "1.0,2.0",
  "USER_AUTH_TYPE": "Local",
  "USE_SFTPD": "True",
  "UseGlobalMailbox": "False",
  "VERSION_NUMBER": "v21.10.01",
  "db_url": "%s",
  "disable_contextmenu": "oncontextmenu=\"return false;\"",
  "elk_dashboard_url": "",
"mq_qm" : "nats2",
"mq_host" : "amfv2-nats",
"mq_port" : "4222",
"mq_channel" : "amfv2-nats:4222",
"Apptitle": "AMF Dashboard",
"Organization":"MFTLABS_AMF"
}`,dburl)

	settingId := "072eb030-69e1-473f-8d15-14c0c132f822"
	loc, _ := time.LoadLocation("UTC")
	createdTime := time.Now().In(loc)
	auditInfo := map[string]string{
		"created_by": "CMD",
		"created_on": createdTime.Format("2006-01-02 15:04:05.000000"),
		"last_modified_by": "",
		"last_modified_on": "",
	}
	auditdata,aerr := json.Marshal(auditInfo)
	if aerr!=nil{
		fmt.Printf("\nFailed to marshal audit data%v",err)
		return aerr
	}
	_, err = db.Con.Exec(INSTALL_CONFIG,settingId,"AMF Settings",jsonStr2,auditdata)
	return err
}
func main() {
	var conf string
	var url string
	var schemaFile string
	flag.StringVar(&conf,"conf","service.conf","Config file")
	flag.StringVar(&url,"dburl","postgres://root@amfv2-cockroachdb:26257/amf?sslmode=disable&TimeZone=UTC","Database connection string")
	flag.StringVar(&schemaFile,"schema","amf_schema.sql","Database Schema")
	flag.Parse()
	util := &AmfUtil{}
	util.LoadConfig(conf)
	url2 := util.GetValue2("DEFAULT","database_url",true)
	if url2!="" {
		url = url2
	}
	fmt.Printf("Running DB Init using %s and %s\n",url,schemaFile)
	dbmgr := &DbMgr{}
	err  := dbmgr.CreateSchema(url,schemaFile)
	if err!=nil {
		fmt.Printf("Error occurred while creating database schema:%v\n",err)
	} else {
		fmt.Printf("Database schema successfully created\n")
		err = dbmgr.InitUiSettings(url)
		if err!=nil {
			fmt.Printf("Failed to initialize ui settings\n")
		}
	}


}