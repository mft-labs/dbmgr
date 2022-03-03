package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"strings"
	"time"
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
	fmt.Printf("Installing schema\n")
	tablesList := make([]string,0)
	tablesCount := 0
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
			return fmt.Errorf( "Cant create statement :%s\n%v\n", statement,err)
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
	//contents2, err := util.ReadFile(BASEPATH+"/amf/validations/table_validations.txt")
	//tablesList := string(contents2)
	verifyTables  := make(map[string]bool)
	//for _, tablename := range strings.Split(tablesList,"\n") {
	for _, tablename := range tablesList {
		verifyTables[strings.TrimSpace(tablename)] = true
	}

	rows, err := db.Con.Query(validationQuery)
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
	return nil
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
	}

}