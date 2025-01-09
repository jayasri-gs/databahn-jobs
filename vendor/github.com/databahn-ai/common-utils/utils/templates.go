package utils

import (
	"bytes"
	"text/template"

	"github.com/Masterminds/sprig/v3"
)

var funcMap = template.FuncMap{
	"minus": minus,
}

func minus(a, b int) int {
	return a - b
}

func ParseTemplate(contents []byte, object interface{}) ([]byte, error) {
	tpl, err := template.New("template").Funcs(funcMap).Funcs(sprig.TxtFuncMap()).Parse(string(contents))
	if err != nil {
		return nil, err
	}
	buf := &bytes.Buffer{}
	err = tpl.Execute(buf, object)

	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ParseTemplateWithHtml(contents []byte, object interface{}) ([]byte, error) {
	tpl, err := template.New("template").Funcs(funcMap).Funcs(sprig.FuncMap()).Parse(string(contents))
	if err != nil {
		return nil, err
	}
	buf := &bytes.Buffer{}
	err = tpl.Execute(buf, object)

	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
