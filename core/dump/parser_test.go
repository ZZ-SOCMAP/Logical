package dump

import (
	"io"
	"reflect"
	"testing"

	"github.com/hellobike/amazonriver/model"
)

func Test_parser_parseWalData(t *testing.T) {
	type fields struct {
		r io.Reader
	}
	type args struct {
		line string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *model.WalData
	}{
		{
			name: "test1",
			fields: fields{
				r: nil,
			},
			args: args{
				line: `INSERT INTO test.test_table (id, name) VALUES (1,"amazonriver");`,
			},
			want: &model.WalData{
				OperationType: model.Insert,
				Schema:        "test",
				Table:         "test_table",
				Data:          map[string]interface{}{"id": int64(1), "name": "amazonriver"},
				Timestamp:     0,
				Pos:           0,
				Rule:          nil,
			},
		},
		{
			name: "test2",
			fields: fields{
				r: nil,
			},
			args: args{
				line: `DELETE FROM test.test_table WHERE id = 1;`,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &parser{
				r: tt.fields.r,
			}
			if got := p.parseSql(tt.args.line); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parser.parseWalData() = %v, want %v", got, tt.want)
			}
		})
	}
}
