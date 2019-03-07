package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func Test_RunVersionCmdNormal(t *testing.T) {
	cmdUser := VersionCommand()
	err := runVersionCmd(cmdUser, []string{})
	require.NoError(t, err)
}

func Test_RunVersionOutput(t *testing.T) {
	cmdUser := VersionCommand()

	mockOut := bytes.NewBufferString("")
	mockErr := bytes.NewBufferString("")
	cmdUser.SetOut(mockOut)
	cmdUser.SetErr(mockErr)

	err := runVersionCmd(cmdUser, []string{})

	require.NoError(t, err)
	assert.Equal(t, "s3mini <unknown commit>\nVersion: 0.1.0\n", mockOut.String())
}
