package lakefs_sdk

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

var key = "AKIAJ4HPAEQI42RCTRKQ"
var serKey = "FFmNdpVv7kpHe7OKq27Y3KTguCYVKmnMeclhClnv"

var urlr = "http://192.168.31.20:8000"

func init() {
	key = "AKIAJPLHGBNSL3JL5UUQ"
	serKey = "wnpeLpodovMgbqTVb3+hs1cjkPZkMPZGofz0LRvB"

	urlr = "http://192.168.88.203:8011"

	key = "AKIAJPLHGBNSL3JL5UUQ"
	serKey = "wnpeLpodovMgbqTVb3+hs1cjkPZkMPZGofz0LRvB"

	urlr = "http://192.168.88.203:8011"
}

func TestLakeFsObjectCommits(t *testing.T) {
	sdk, err := New(urlr, key, serKey, time.Second*30)
	if err != nil {
		log.Fatalln(err)
	}

	diff, err := sdk.GetObjectHistoryCommits("test2", "main", "0006131de8c5938a492959c8262b358a.txt")
	if err != nil {
		panic(err)
	}

	print(diff)
}

func TestLakeFsObjectCommits22(t *testing.T) {
	sdk, err := New(urlr, key, serKey, time.Second*300)
	if err != nil {
		log.Fatalln(err)
	}

	now := time.Now()
	diff, err := sdk.CreateCommit("test3", "main", CommitMessage{
		Message: "test2 commit test2",
		Metadata: map[string]string{
			"v1": "vv222",
		},
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(time.Since(now).Milliseconds())

	print(diff)
}

func TestLakeFsUut(t *testing.T) {
	sdk, err := New(urlr, key, serKey, time.Second*30)
	if err != nil {
		log.Fatalln(err)
	}

	diff, err := sdk.UploadObject("base", "main", "aaa.txt", []byte("asdsadasdwefwefwef"))
	if err != nil {
		panic(err)
	}

	print(diff)
}

func TestLakeFsDiff(t *testing.T) {
	sdk, err := New(urlr, key, serKey, time.Second)
	if err != nil {
		log.Fatalln(err)
	}

	diff, err := sdk.Diff("base", "main", "7c302319c989c5d769b8ac874cab21ded8f53798e69e41116b4de806fafc3fff")
	if err != nil {
		panic(err)
	}

	print(diff)
}

func TestLakeFsUploadObjectAndSetMetaData(t *testing.T) {
	sdk, err := New(urlr, key, serKey, time.Second*5)
	if err != nil {
		log.Fatalln(err)
	}

	file, err := ioutil.ReadFile("model.go")
	if err != nil {
		log.Fatalln(err)
	}
	err = sdk.UploadObjectAndSetMetaData("base", "main", "asd/model.go", file, map[string]string{
		"v1": "v22",
		"v2": "v22",
		"v3": "v22",
	})
	if err != nil {
		log.Fatalln(err)
	}

	data, err := sdk.ObjectMetaData("lakefs-test", "main", "asd/model.go")
	if err != nil {
		log.Fatalln(err)
	}

	print(data)
}

func TestLakeFsSDKUpload(t *testing.T) {
	sdk, err := New(urlr, key, serKey, time.Second)
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

	//metadata, err := sdk.PutObject("base", "main", "a15b4afegy1fmvjq0djc7j21hc0u0tq4.jpeg", SetMetadata{
	//	PhysicalAddress: "s3://lakefs/62608ece34424c1b818d3f8b0a6fadaf",
	//	Metadata: map[string]string{
	//		"v1": "vv",
	//		"v2": "vv2",
	//		"v3": "vv3",
	//	},
	//})
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//
	//print(metadata)

	data, err := sdk.ObjectMetaData("base", "main", "gb.pdf")
	if err != nil {
		log.Fatalln(err)
	}

	print(data)

	//metadata, err := sdk.PutObject("base", "main", "gb.pdf", SetMetadata{
	//	PhysicalAddress: data.PhysicalAddress,
	//	Metadata: map[string]string{
	//		"v1": "vv",
	//		"v2": "vv2",
	//		"v3": "vv3",
	//	},
	//	Checksum:    data.Checksum,
	//	SizeBytes:   data.SizeBytes,
	//	Mtime:       data.Mtime,
	//	ContentType: data.ContentType,
	//})
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//
	//print(metadata)
}
func TestLakeFsSDK1(t *testing.T) {
	sdk, err := New(urlr, key, serKey, time.Second)
	if err != nil {
		log.Fatalln(err)
	}

	branch, err := sdk.GetBranch("demo", "main")
	if err != nil {
		log.Fatalln(err)
	}

	print(branch)

	object, err := sdk.ListObject("demo", branch.CommitId, "", 100)
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
	sdk, err := New(urlr, key, serKey, time.Second)
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
	sdk, err := New(urlr, key, serKey, time.Second)
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
	sdk, err := New(urlr, key, serKey, time.Second)
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
	sdk, err := New(urlr, key, serKey, time.Second)
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
	sdk, err := New(urlr, key, serKey, time.Second)
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

	lss, err := sdk.ListObject("base", branch.CommitId, "", 1000)
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

func TestTime(t *testing.T) {
	ti := time.NewTicker(time.Second)
	for {
		select {
		case <-ti.C:
			fmt.Println("a")
		}
	}
}

func TestListObjectPrefix(t *testing.T) {
	sdk, err := New("http://192.168.88.203:8011", "AKIAJOJ646YH42F6SGVQ", "AXIBSfcWCjt75FQvWtEqgVRGSkfEAB/8fSY1+6gT", 0)
	if err != nil {
		panic(err)
	}

	//object, err := sdk.ListObject("lakefs-static-official", "main", "", 1000)
	//if err != nil {
	//	panic(err)
	//}
	//
	//fmt.Println(len(object.Results))

	path := `rime_data_caesar_high_tech_canceled/Z2FveGluX3F1eGlhbw==/2022/`

	after := ""
	for {
		object, err := sdk.ListObjectPrefix("lakefs-static-official", "main", path, after, 1000)
		if err != nil {
			panic(err)
		}

		fmt.Println(len(object.Results))
		//Print(object)

		if object.Pagination.NextOffset == "" {
			break
		}
		fmt.Println("Next: ", object.Pagination.NextOffset)
		after = object.Pagination.NextOffset

		for _, v := range object.Results {
			Print(v)
			getObject, err := sdk.GetObject("lakefs-static-official", "main", v.Path)
			if err != nil {
				panic(err)
			}

			if !strings.Contains(v.Path, ".index") {
				continue
			}

			var t Tt
			err = json.Unmarshal(getObject, &t)
			if err != nil {
				log.Println(err)
				log.Println(string(getObject))
				continue
			}

			os.Exit(0)
		}
	}
}

type Tt struct {
	Id              string    `json:"id"`
	Title           string    `json:"title"`
	Source          string    `json:"source"`
	PublishedDate   int       `json:"published_date"`
	Provider        string    `json:"provider"`
	ProviderAssetId string    `json:"provider_asset_id"`
	IsDynamic       bool      `json:"is_dynamic"`
	ListPath        []string  `json:"list_path"`
	CreatedAt       time.Time `json:"created_at"`
}

func Print(i interface{}) {
	marshal, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(marshal))
}
