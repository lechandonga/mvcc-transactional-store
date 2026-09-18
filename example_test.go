package mvcc_test

import (
	"errors"
	"fmt"
	"os"

	mvcc "github.com/lechandonga/mvcc-transactional-store"
)

func tmpDir() string {
	d, err := os.MkdirTemp("", "mvcc-example-")
	if err != nil {
		panic(err)
	}
	return d
}

func Example() {
	dir := tmpDir()
	store, err := mvcc.Open(&mvcc.Options{Dir: dir})
	if err != nil {
		panic(err)
	}
	defer store.Close()

	err = store.Update(func(tx *mvcc.Txn) error {
		if err := tx.Put("alpha", []byte("1")); err != nil {
			return err
		}
		return tx.Put("beta", []byte("2"))
	})
	fmt.Println("commit:", err)

	_ = store.View(func(tx *mvcc.Txn) error {
		rows, err := tx.Scan("a", "c")
		if err != nil {
			return err
		}
		for _, r := range rows {
			fmt.Printf("%s=%s\n", r.Key, string(r.Value))
		}
		return nil
	})

	// 两个基于同一快照的并发事务写同一键：先提交者胜。
	t1, _ := store.Begin(false)
	t2, _ := store.Begin(false)
	_ = t1.Put("alpha", []byte("one"))
	_ = t2.Put("alpha", []byte("two"))
	_ = t1.Commit()
	err = t2.Commit()
	var ce *mvcc.ConflictError
	if errors.As(err, &ce) {
		fmt.Printf("conflict on %s: winner=%d loser=%d\n", ce.Key, ce.WinnerID, ce.LoserID)
	}

	// Output:
	// commit: <nil>
	// alpha=1
	// beta=2
	// conflict on alpha: winner=4 loser=5
}
