package lakefs_sdk

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

var key = "AKIAJ4HPAEQI42RCTRKQ"
var serKey = "pOZO1EWeU1UKM8hGdTxXwj5OLBcR7Bgzd4Ue6CfT"

var url = "http://192.168.31.20:8000"

func TestLakeFsSDKUpload(t *testing.T) {
	sdk, err := New(url, key, serKey)
	if err != nil {
		log.Fatalln(err)
	}

	//file, err := ioutil.ReadFile("xxx.png")
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//object, err := sdk.UploadObject("demo", "main", "1o1o.png", file)
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//
	//print(object)

	metadata, err := sdk.PutObject("demo", "main", "1o1o.png", SetMetadata{
		PhysicalAddress: "1o1o.png",
		Metadata: map[string]string{
			"v1": "vv",
			"v2": "vv2",
			"v3": "vv3",
		},
	})
	if err != nil {
		log.Fatalln(err)
	}

	print(metadata)
}
func TestLakeFsSDK1(t *testing.T) {
	sdk, err := New(url, key, serKey)
	if err != nil {
		log.Fatalln(err)
	}

	branch, err := sdk.GetBranch("demo", "main")
	if err != nil {
		log.Fatalln(err)
	}

	print(branch)

	object, err := sdk.ListObject("demo", branch.CommitId)
	if err != nil {
		log.Fatalln(err)
	}

	print(object)

	for _, v := range object.Results {
		properties, err := sdk.UnderlyingProperties("demo", branch.CommitId, v.Path)
		if err != nil {
			log.Fatalln(err)
		}

		print(properties)

		data, err := sdk.ObjectMetaData("demo", branch.CommitId, v.Path)
		if err != nil {
			log.Fatalln(err)
		}

		print(data)

		//if data.Path == "1000.png" {
		//	err := sdk.DeleteObject("demo", "main", data.Path)
		//	if err != nil {
		//		log.Fatalln(err)
		//	}
		//}

		if data.Path == "1000.png" {
			getObject, err := sdk.GetObject("demo", branch.CommitId, data.Path)
			if err != nil {
				log.Fatalln(err)
			}

			ioutil.WriteFile("xxx.png", getObject, 00666)
		}
	}
}

func TestLakeFsSDK2(t *testing.T) {
	sdk, err := New(url, key, serKey)
	if err != nil {
		log.Fatalln(err)
	}

	repositories, err := sdk.Repositories()
	if err != nil {
		log.Fatalln(err)
	}

	for _, v := range repositories.Results {
		fmt.Println(v.StorageNamespace)
		fmt.Println(v.Id)

		err = sdk.CreateBranch(v.Id, "v3", "v2")
		if err != nil {
			log.Fatalln(err)
		}

		branches, err := sdk.Branches(v.Id)
		if err != nil {
			log.Fatalln(err)
		}

		fmt.Println(branches)
	}
}

func TestLakeFsSDK3(t *testing.T) {
	sdk, err := New(url, key, serKey)
	if err != nil {
		log.Fatalln(err)
	}

	repositories, err := sdk.Repositories()
	if err != nil {
		log.Fatalln(err)
	}

	for _, v := range repositories.Results {
		fmt.Println(v.StorageNamespace)
		fmt.Println(v.Id)

		branches, err := sdk.Branches(v.Id)
		if err != nil {
			log.Fatalln(err)
		}

		for _, v2 := range branches.Results {
			branch, err := sdk.GetBranch(v.Id, v2.Id)
			if err != nil {
				log.Fatalln(err)
			}

			fmt.Println("===")
			fmt.Println(v.Id)
			fmt.Println(v2.Id)
			fmt.Println(branch)
			if v2.Id == "v3" {
				sdk.DeleteBranch(v.Id, v2.Id)
			}
		}
	}
}

func TestLakeFsSDK4(t *testing.T) {
	sdk, err := New(url, key, serKey)
	if err != nil {
		log.Fatalln(err)
	}

	commit, err := sdk.CreateCommit("base", "main", CommitMessage{
		Message: "this is commit message",
		Metadata: map[string]string{
			"asd":  "asd",
			"asd1": "as2d",
			"asd2": "a3sd",
			"asd3": "as4d",
		},
	})
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(commit)
}

func TestLakeFsSDK5(t *testing.T) {
	sdk, err := New(url, key, serKey)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("asdasdsadsad")

	commits, err := sdk.GetCommits("base", "d44c570e5e94c07bfc1f6ff6025a349bc10dfc23ebfa5e05619bf1ad4faec0c6")
	if err != nil {
		log.Fatalln(err)
	}

	marshal, err := json.Marshal(commits)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(string(marshal))
}

func TestLakeFsSDK6(t *testing.T) {
	sdk, err := New(url, key, serKey)
	if err != nil {
		log.Fatalln(err)
	}

	branch, err := sdk.GetBranch("base", "main")
	if err != nil {
		log.Fatalln(err)
	}

	marshal, err := json.Marshal(branch)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(string(marshal))

	lss, err := sdk.ListObject("base", branch.CommitId)
	if err != nil {
		log.Fatalln(err)
	}

	print(lss)
}

func print(i interface{}) {
	marshal, err := json.Marshal(i)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(string(marshal))
}

type N struct {
	Name string
}

func TestP2(t *testing.T) {
	ns := make(map[string]*N, 0)

	for i := 0; i < 100; i++ {
		ns[fmt.Sprintf("this %d", i)] = &N{
			fmt.Sprintf("this %d", i),
		}
	}

	for _, v := range ns {
		p := v
		go func() {
			fmt.Println(p.Name)
		}()
	}

	time.Sleep(time.Second * 3)
}

func TestP(t *testing.T) {
	ns := make([]*N, 0)

	for i := 0; i < 100; i++ {
		ns = append(ns, &N{
			fmt.Sprintf("this %d", i),
		})
	}

	for _, v := range ns {

		p := v
		go func() {
			fmt.Println(p.Name)
		}()
	}

	time.Sleep(time.Second * 3)
}
