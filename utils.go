package main

import (
	"fmt"
	"gopkg.in/ini.v1"
	"strings"
)

type AmfUtil struct {
	Config *ini.File
}
func (u *AmfUtil) LoadConfig(conf string) (err error) {
	u.Config, err = ini.Load(conf)
	if err!=nil {
		u.Config = nil
		return
	}
	return
}

func (u *AmfUtil) GetValue(sec, key string) (string) {
	section, err := u.Config.GetSection(sec)
	if err != nil {
		fmt.Printf("Section not found:%s (%v)\n",sec,err)
		return ""
	}
	key2, err := section.GetKey(key)
	if err!=nil {
		fmt.Printf("Key not found:%s (%v)\n", key, err)
		return ""
	}
	return strings.TrimSpace(key2.Value())
}

func (u *AmfUtil) GetValue2(sec, key string,silence bool) (string) {
	section, err := u.Config.GetSection(sec)
	if err != nil {
		if silence {
			return ""
		}
		fmt.Printf("Section not found:%s (%v)\n",sec,err)
		return ""
	}
	key2, err := section.GetKey(key)
	if err!=nil {
		if silence {
			return ""
		}
		fmt.Printf("Key not found:%s (%v)\n", key, err)
		return ""
	}
	return strings.TrimSpace(key2.Value())
}
