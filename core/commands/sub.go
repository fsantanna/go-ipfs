package commands

import (
	"fmt"
	"bytes"
	"io"
	"reflect"
	cmds "github.com/ipfs/go-ipfs/commands"
	key "github.com/ipfs/go-ipfs/blocks/key"
	bitswap "github.com/ipfs/go-ipfs/exchange/bitswap"
	sublist "github.com/ipfs/go-ipfs/exchange/bitswap/sublist"
	u "github.com/ipfs/go-ipfs/util"
)

var SubCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "TODO",
		ShortDescription: `
TODO.
`,
	},

	Arguments: []cmds.Argument{
		cmds.StringArg("key", true, true, "keys to subscribe").EnableStdin(),
	},
	Marshalers: cmds.MarshalerMap{
		cmds.Text: func(res cmds.Response) (io.Reader, error) {
			outChan, ok := res.Output().(<-chan interface{})
			if !ok {
				fmt.Println(reflect.TypeOf(res.Output()))
				return nil, u.ErrCast()
			}

			marshal := func(v interface{}) (io.Reader, error) {
				obj, ok := v.(string)
				if !ok {
					return nil, u.ErrCast()
				}

				buf := new(bytes.Buffer)
				fmt.Fprintf(buf, "%s\n", obj)
				return buf, nil
			}

			return &cmds.ChannelMarshaler{
				Channel:   outChan,
				Marshaler: marshal,
				Res:       res,
			}, nil
		},
	},
	Run: func(req cmds.Request, res cmds.Response) {
		nd, err := req.InvocContext().GetNode()
		if err != nil {
			res.SetError(err, cmds.ErrNormal)
			return
		}

		if !nd.OnlineMode() {
			res.SetError(errNotOnline, cmds.ErrClient)
			return
		}

		bs, ok := nd.Exchange.(*bitswap.Bitswap)
		if !ok {
			res.SetError(u.ErrCast(), cmds.ErrNormal)
			return
		}

		ts := []sublist.Topic{}
		for _, arg := range req.Arguments() {
			topic := sublist.Topic(arg)
			ts = append(ts, topic)
		}

		outChan := make(chan interface{})
		res.SetOutput((<-chan interface{})(outChan))

		go func() {
			defer close(outChan)
			keys := make(chan key.Key)
			bs.SubTopics(ts, keys)

			for key := range keys {
				outChan <- &key
			}
		}()
	},
}
