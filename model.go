package lakefs_sdk

type token struct {
	Token string `json:"token"`
}

type Repositories struct {
	Pagination struct {
		HasMore    bool   `json:"has_more"`
		NextOffset string `json:"next_offset"`
		Results    int    `json:"results"`
		MaxPerPage int    `json:"max_per_page"`
	} `json:"pagination"`
	Results []struct {
		Id               string `json:"id"`
		CreationDate     int    `json:"creation_date"`
		DefaultBranch    string `json:"default_branch"`
		StorageNamespace string `json:"storage_namespace"`
	} `json:"results"`
}

type CreateRepositoriesResponse struct {
	Id               string `json:"id"`
	CreationDate     int    `json:"creation_date"`
	DefaultBranch    string `json:"default_branch"`
	StorageNamespace string `json:"storage_namespace"`
}

type GetRepositoriesResponse struct {
	Id               string `json:"id"`
	CreationDate     int    `json:"creation_date"`
	DefaultBranch    string `json:"default_branch"`
	StorageNamespace string `json:"storage_namespace"`
}

type Branches struct {
	Pagination struct {
		HasMore    bool   `json:"has_more"`
		NextOffset string `json:"next_offset"`
		Results    int    `json:"results"`
		MaxPerPage int    `json:"max_per_page"`
	} `json:"pagination"`
	Results []struct {
		Id       string `json:"id"`
		CommitId string `json:"commit_id"`
	} `json:"results"`
}

type Branch struct {
	Id       string `json:"id"`
	CommitId string `json:"commit_id"`
}

type CommitMessage struct {
	Message  string            `json:"message"`
	Metadata map[string]string `json:"metadata"`
}

type CommitResp struct {
	Id           string            `json:"id"`
	Parents      []string          `json:"parents"`
	Committer    string            `json:"committer"`
	Message      string            `json:"message"`
	CreationDate int               `json:"creation_date"`
	MetaRangeId  string            `json:"meta_range_id"`
	Metadata     map[string]string `json:"metadata"`
}
