package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

type Binary struct {
	Name     string `json:"name"`
	FileName string `json:"file_name"`
	File     string `json:"file"`
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	SigFile  string `json:"sig"`
	Checksum string `json:"checksum"`
	Commit   string `json:"commit"`
	Version  string `json:"version"`
	Tag      string `json:"tag"`
	Release  string `json:"release"`
}

type Release struct {
	Version  string   `json:"version"`
	Binaries []Binary `json:"binaries"`
}

type Releases struct {
	Releases []Release `json:"releases"`
}

type GithubReleaseAsset struct {
	Url                string `json:"url"`
	Name               string `json:"name"`
	BrowserDownloadUrl string `json:"browser_download_url"`
	Size               int    `json:"size"`
	ContentType        string `json:"content_type"`
}

type GithubRelease struct {
	TagName     string               `json:"tag_name"`
	Name        string               `json:"name"`
	PublishedAt time.Time            `json:"published_at"`
	Assets      []GithubReleaseAsset `json:"assets"`
}

type DockerImage struct {
	Creator int `json:"creator"`
	Id      int `json:"id"`
	Images  []struct {
		Architecture string      `json:"architecture"`
		Features     string      `json:"features"`
		Variant      interface{} `json:"variant"`
		Digest       string      `json:"digest"`
		Os           string      `json:"os"`
		OsFeatures   string      `json:"os_features"`
		OsVersion    interface{} `json:"os_version"`
		Size         int         `json:"size"`
		Status       string      `json:"status"`
		LastPulled   time.Time   `json:"last_pulled"`
		LastPushed   time.Time   `json:"last_pushed"`
	} `json:"images"`
	LastUpdated         time.Time `json:"last_updated"`
	LastUpdater         int       `json:"last_updater"`
	LastUpdaterUsername string    `json:"last_updater_username"`
	Name                string    `json:"name"`
	Repository          int       `json:"repository"`
	FullSize            int       `json:"full_size"`
	V2                  bool      `json:"v2"`
	TagStatus           string    `json:"tag_status"`
	TagLastPulled       time.Time `json:"tag_last_pulled"`
	TagLastPushed       time.Time `json:"tag_last_pushed"`
	MediaType           string    `json:"media_type"`
	ContentType         string    `json:"content_type"`
	Digest              string    `json:"digest"`
}

var githubToken string
var releasesCount int

func init() {
	flag.StringVar(&githubToken, "github-token", "", "Github token")
	flag.IntVar(&releasesCount, "releases", 5, "Number of releases to fetch")

	flag.Parse()

	// check all flags are set
	flag.VisitAll(func(f *flag.Flag) {
		if f.Value.String() == "" {
			fmt.Printf("Flag %s is not set\n", f.Name)
			os.Exit(1)
		}
	})
}

// release is a small program which takes command line flags from a ci release job for an individual binary
// and adds the binary to the releases.json file which is then served by downloads.erigon.ch
func main() {
	// read releases from disk (same file as used to populate downloads.erigon.ch)
	releases, err := readJsonFile()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// get last 5 releases
	ghReleases, err := getReleases(githubToken, "https://api.github.com/repos/ledgerwatch/erigon/releases", releasesCount)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// loop binaries to get stable and rc versions
	var stable []GithubRelease
	var rc []GithubRelease
	for _, ghRelease := range ghReleases {
		if strings.Contains(ghRelease.TagName, "rc") {
			if len(rc) < releasesCount {
				rc = append(rc, ghRelease)
			}
		} else {
			if len(stable) < releasesCount {
				stable = append(stable, ghRelease)
			}
		}
	}

	allReleases := append(stable, rc...)

	for _, ghRelease := range allReleases {
		checksums := map[string]string{}

		// get checksums first
		for _, asset := range ghRelease.Assets {
			if strings.Contains(asset.Name, "checksums") {
				checksums, err = parseChecksums(asset)
				if err != nil {
					fmt.Println(err)
					continue
				}
			}
		}

		for _, asset := range ghRelease.Assets {
			if strings.Contains(asset.Name, "checksums") {
				continue
			}

			name, v, o, arch, err := parseAssetName(asset)
			binary := Binary{
				Name:     name,
				File:     asset.BrowserDownloadUrl,
				FileName: asset.Name,
				OS:       o,
				Arch:     arch,
				SigFile:  "",
				Checksum: checksums[asset.Name],
				Commit:   "",
				Version:  v,
				Tag:      ghRelease.TagName,
				Release:  ghRelease.Name,
			}

			// add current binary to releases (if doesn't already exist)
			releases, err = addCurrentBinary(releases, binary)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}

		v := strings.TrimPrefix(ghRelease.TagName, "v")

		// add docker images
		repositories, err := getDockerImages("https://hub.docker.com/v2/repositories/thorax/erigon/tags", v)
		if err != nil {
			fmt.Println(err)
			continue
		}
		for _, repo := range repositories {
			file := fmt.Sprintf("https://hub.docker.com/r/thorax/erigon/tags?page=1&ordering=last_updated&name=%s", repo.Name)
			binary := Binary{
				Name:     repo.Name,
				File:     file,
				FileName: fmt.Sprintf("%s (%s, %s)", repo.Name, repo.Images[0].Os, repo.Images[0].Architecture),
				OS:       "docker",
				Arch:     repo.Images[0].Architecture,
				SigFile:  "",
				Checksum: repo.Images[0].Digest,
				Commit:   "",
				Version:  v,
				Tag:      ghRelease.TagName,
				Release:  ghRelease.Name,
			}

			releases, err = addCurrentBinary(releases, binary)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}

	// sort releases descending by version
	sort.Slice(releases.Releases, func(i, j int) bool {
		return releases.Releases[i].Version > releases.Releases[j].Version
	})

	// write releases back to disk
	err = writeJsonFile(releases)
}

func getDockerImages(url string, search string) ([]DockerImage, error) {
	var images []DockerImage

	for {
		// Make GET request to DockerHub API
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}

		// Decode JSON response
		var data struct {
			Results []DockerImage `json:"results"`
			Next    string        `json:"next"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			return nil, err
		}
		resp.Body.Close()

		// Append images with matching prefix to results
		for _, image := range data.Results {
			if strings.Contains(image.Name, search) {
				images = append(images, image)
			}
		}

		// Stop if there are no more pages to retrieve
		if data.Next == "" {
			break
		}

		// Update URL to retrieve next page
		url = data.Next
	}

	return images, nil
}

func getReleases(token string, releasesUrl string, count int) ([]GithubRelease, error) {
	// make request to github api to get release at url using token
	req, err := http.NewRequest("GET", releasesUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("Per-Page", fmt.Sprintf("%d", count))
	req.Header.Set("Page", "1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	var releases []GithubRelease
	err = json.NewDecoder(resp.Body).Decode(&releases)
	if err != nil {
		return nil, fmt.Errorf("error parsing json: %w", err)
	}

	return releases, nil

}

func getLatestRelease(token string, releaseUrl string) (GithubRelease, error) {
	// make request to github api to get release at url using token
	req, err := http.NewRequest("GET", releaseUrl, nil)
	if err != nil {
		return GithubRelease{}, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return GithubRelease{}, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	var release GithubRelease
	err = json.NewDecoder(resp.Body).Decode(&release)
	if err != nil {
		return GithubRelease{}, fmt.Errorf("error parsing json: %w", err)
	}

	return release, nil
}

func parseAssetName(asset GithubReleaseAsset) (name string, version string, os string, arch string, err error) {
	// parse asset name to get binary name, os and arch
	// example: erigon_2.39.0_darwin_amd64.tar.gz

	fileName := strings.TrimSuffix(asset.Name, ".tar.gz")
	list := strings.Split(fileName, "_")
	if len(list) != 4 {
		if strings.Contains(asset.Name, "checksum") {
			// tbc
			return "checksums", list[1], "", "", nil
		}
		return "", "", "", "", fmt.Errorf("error parsing asset name: %s", asset.Name)
	}

	return list[0], list[1], list[2], list[3], nil
}

func parseChecksums(asset GithubReleaseAsset) (map[string]string, error) {
	// download checksum file
	resp, err := http.Get(asset.BrowserDownloadUrl)
	if err != nil {
		return nil, fmt.Errorf("error downloading checksum file: %w", err)
	}

	// parse checksum file
	checksums := map[string]string{}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		list := strings.Split(line, " ")
		if len(list) != 3 {
			return nil, fmt.Errorf("error parsing checksum file: %s", line)
		}
		checksums[list[2]] = list[0]
	}

	return checksums, nil
}

func readJsonFile() (Releases, error) {
	data, err := os.ReadFile("./releases.json")
	if err != nil {
		if os.IsNotExist(err) {
			return Releases{}, nil
		}
		return Releases{}, fmt.Errorf("error reading file: %w", err)
	}

	var releases Releases
	err = json.Unmarshal(data, &releases)
	if err != nil {
		return Releases{}, fmt.Errorf("error parsing json: %w", err)
	}
	return releases, nil
}

func addCurrentBinary(releases Releases, b Binary) (Releases, error) {
	// check if the binary is already in the release, if so return (no change to be made)
	for _, release := range releases.Releases {
		for _, binary := range release.Binaries {
			if binary.Name == b.Name && binary.OS == b.OS && binary.Arch == b.Arch && binary.Version == b.Version {
				return releases, fmt.Errorf("binary already exists in release")
			}
		}
	}

	var found bool
	// add binary to existing release
	for i, release := range releases.Releases {
		if release.Version == b.Version {
			releases.Releases[i].Binaries = append(releases.Releases[i].Binaries, b)
			found = true
			break
		}
	}

	// if no releases exist or there isn't one for the current binary version, create one
	if len(releases.Releases) == 0 || !found {
		releases.Releases = append(releases.Releases, Release{
			Version:  b.Version,
			Binaries: []Binary{b},
		})
	}

	// sort releases by version
	sort.Slice(releases.Releases, func(i, j int) bool {
		v1, _ := version.NewVersion(releases.Releases[i].Version)
		v2, _ := version.NewVersion(releases.Releases[j].Version)
		return v1.LessThan(v2)
	})

	// remove the oldest release to maintain a list of 5
	if len(releases.Releases) > releasesCount {
		releases.Releases = releases.Releases[1:]
	}

	return releases, nil
}

func writeJsonFile(releases Releases) error {
	data, err := json.MarshalIndent(releases, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshalling json: %w", err)
	}

	err = os.WriteFile("releases.json", data, 0644)
	if err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}

	return nil
}
