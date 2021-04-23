package handler

import (
	"fmt"
	"google.golang.org/grpc"
)

// GetRpcConnect 与预测RPC服务器建立连接
func GetRpcConnect(host string) (conn *grpc.ClientConn, err error) {
	if conn, err = grpc.Dial(host, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithReturnConnectionError(), grpc.FailOnNonTempDialError(true)); err != nil {
		return nil, fmt.Errorf("连接预测服务器失败(%s), 请联系系统管理员", host)
	}
	return conn, err
}

// retry retry util job succeeded or retry count limit exceed
func retry(retry int, job func() error) error {
	var count int
	for {
		count++
		if err := job(); err != nil {
			if retry == -1 || (retry > 0 && count <= retry) {
				continue
			}
			return err
		}
		return nil
	}
}

// match finds whether the text matches/satisfies the pattern string.
func match(pattern, name string) bool {
	if pattern == "" {
		return name == pattern
	}
	if pattern == "*" {
		return true
	}
	runeNames := make([]rune, 0, len(name))
	runePatterns := make([]rune, 0, len(pattern))
	for _, r := range name {
		runeNames = append(runeNames, r)
	}
	for _, r := range pattern {
		runePatterns = append(runePatterns, r)
	}
	return deepMatchRune(runeNames, runePatterns, true)
}

func deepMatchRune(str, pattern []rune, simple bool) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		default:
			if len(str) == 0 || str[0] != pattern[0] {
				return false
			}
		case '?':
			if len(str) == 0 && !simple {
				return false
			}
		case '*':
			return deepMatchRune(str, pattern[1:], simple) ||
				(len(str) > 0 && deepMatchRune(str[1:], pattern, simple))
		}
		str = str[1:]
		pattern = pattern[1:]
	}
	return len(str) == 0 && len(pattern) == 0
}
