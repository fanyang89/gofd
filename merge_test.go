package main

import (
	"os"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"
)

type MergeTestSuite struct {
	suite.Suite
}

func TestMerge(t *testing.T) {
	suite.Run(t, new(MergeTestSuite))
}

func (s *MergeTestSuite) TestMergeDirectory() {
	fs := afero.NewMemMapFs()

	// create gallery dirs
	err := fs.Mkdir("/gallery", 0755)
	s.Require().NoError(err)
	err = fs.Mkdir("/gallery/a", 0755)
	s.Require().NoError(err)
	err = fs.Mkdir("/gallery/b", 0755)
	s.Require().NoError(err)
	err = fs.Mkdir("/gallery/c", 0755)
	s.Require().NoError(err)

	// create tmp dirs
	err = fs.Mkdir("/tmp", 0755)
	s.Require().NoError(err)
	err = fs.Mkdir("/tmp/a", 0755)
	s.Require().NoError(err)
	err = fs.Mkdir("/tmp/b", 0755)
	s.Require().NoError(err)
	err = fs.Mkdir("/tmp/c", 0755)
	s.Require().NoError(err)

	// create files
	f, err := fs.Create("/gallery/a/1.txt")
	s.Require().NoError(err)
	_ = f.Close()
	f, err = fs.Create("/tmp/b/2.txt")
	s.Require().NoError(err)
	_ = f.Close()
	f, err = fs.Create("/tmp/c/3.txt")
	s.Require().NoError(err)
	_ = f.Close()

	// do merge
	err = mergePath(fs, "/gallery", "/tmp", false)
	s.Require().NoError(err)

	// check
	_, err = fs.Stat("/gallery/a/1.txt")
	s.Require().NoError(err)
	_, err = fs.Stat("/gallery/b/2.txt")
	s.Require().NoError(err)
	_, err = fs.Stat("/gallery/c/3.txt")
	s.Require().NoError(err)
	_, err = fs.Stat("/tmp/a/1.txt")
	s.True(os.IsNotExist(err))
	_, err = fs.Stat("/tmp/b/2.txt")
	s.True(os.IsNotExist(err))
	_, err = fs.Stat("/tmp/c/3.txt")
	s.True(os.IsNotExist(err))
}
