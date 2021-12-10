package lakefs_sdk

import (
	"errors"
	"fmt"

	"github.com/dollarkillerx/urllib"
)

type LakeFsSdk struct {
	addr            string
	accessKeyID     string
	secretAccessKey string
	token           string
}

// New 初始化 LakeFsSdk
func New(addr string, accessKeyID string, secretAccessKey string) (*LakeFsSdk, error) {
	l := LakeFsSdk{
		addr:            addr,
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
	}

	// login
	var t token

	err := urllib.Post(fmt.Sprintf("%s/api/v1/auth/login", l.addr)).
		SetJsonObject(map[string]string{
			"access_key_id":     l.accessKeyID,
			"secret_access_key": l.secretAccessKey,
		}).FromJson(&t)
	if err != nil {
		return nil, err
	}

	l.token = t.Token

	return &l, nil
}

func (l *LakeFsSdk) auth(url *urllib.Urllib) *urllib.Urllib {
	if url == nil {
		return url
	}

	return url.SetHeader("Cookie", fmt.Sprintf("access_token=%s", l.token))
}

// Repositories 获取 所有存储库
func (l *LakeFsSdk) Repositories() (*Repositories, error) {
	var resp Repositories
	err := l.auth(urllib.Get(fmt.Sprintf("%s/api/v1/repositories", l.addr))).FromJson(&resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// CreateRepositories 创建存储库
func (l *LakeFsSdk) CreateRepositories(name, storageNamespace, defaultBranch string) (*CreateRepositoriesResponse, error) {
	var resp CreateRepositoriesResponse
	err := l.auth(urllib.Post(fmt.Sprintf("%s/api/v1/repositories", l.addr))).
		SetJsonObject(map[string]string{
			"name":              name,
			"storage_namespace": storageNamespace,
			"default_branch":    defaultBranch,
		}).FromJson(&resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetRepositories 获取 存储库 Repositories
func (l *LakeFsSdk) GetRepositories(repository string) (*GetRepositoriesResponse, error) {
	var resp GetRepositoriesResponse
	err := l.auth(urllib.Get(fmt.Sprintf("%s/api/v1/repositories/%s", l.addr, repository))).
		FromJson(&resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// DeleteRepositories Delete 存储库 Repositories
func (l *LakeFsSdk) DeleteRepositories(repository string) error {
	code, resp, err := l.auth(urllib.Delete(fmt.Sprintf("%s/api/v1/repositories/%s", l.addr, repository))).
		Byte()
	if err != nil {
		return err
	}
	if code != 200 {
		return errors.New(string(resp))
	}

	return nil
}

// Branches 列出所有分支
func (l *LakeFsSdk) Branches(repository string) (*Branches, error) {
	var resp Branches
	err := l.auth(urllib.Get(fmt.Sprintf("%s/api/v1/repositories/%s/branches", l.addr, repository))).
		FromJson(&resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// CreateBranch 创建分支
func (l *LakeFsSdk) CreateBranch(repository string, branchName string, branchSource string) error {
	code, bytes, err := l.auth(urllib.Post(fmt.Sprintf("%s/api/v1/repositories/%s/branches", l.addr, repository))).
		SetJsonObject(map[string]string{
			"name":   branchName,
			"source": branchSource,
		}).Byte()
	if err != nil {
		return err
	}

	if code == 200 || code == 201 {
		return nil
	}
	return errors.New(string(bytes))
}

// GetBranch 获取分支
func (l *LakeFsSdk) GetBranch(repository string, branchName string) (*Branch, error) {
	var br Branch
	err := l.auth(urllib.Get(fmt.Sprintf("%s/api/v1/repositories/%s/branches/%s", l.addr, repository, branchName))).
		FromJson(&br)
	if err != nil {
		return nil, err
	}

	return &br, nil
}

// DeleteBranch 删除分支
func (l *LakeFsSdk) DeleteBranch(repository string, branchName string) error {
	code, bytes, err := l.auth(urllib.Delete(fmt.Sprintf("%s/api/v1/repositories/%s/branches/%s", l.addr, repository, branchName))).Byte()
	if err != nil {
		return err
	}

	if code != 204 {
		return errors.New(string(bytes))
	}

	return nil
}

// CreateCommit 创建 commit
func (l *LakeFsSdk) CreateCommit(repository string, branchName string, message CommitMessage) (*CommitResp, error) {
	var resp CommitResp
	err := l.auth(urllib.Post(fmt.Sprintf("%s/api/v1/repositories/%s/branches/%s/commits", l.addr, repository, branchName))).
		SetJsonObject(message).FromJsonByCode(&resp, 201)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetCommits 获取commits
func (l *LakeFsSdk) GetCommits(repository string, commitID string) (*CommitResp, error) {
	var resp CommitResp
	err := l.auth(urllib.Get(fmt.Sprintf("%s/api/v1/repositories/%s/commits/%s", l.addr, repository, commitID))).FromJsonByCode(&resp, 200)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// ListObject 获取对象列表
func (l *LakeFsSdk) ListObject(repository string, commitID string) (*CommitResp, error) {

}
