package main

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int64, error) {

	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) VALUES (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	p := Parcel{}
	row := s.db.QueryRow("SELECT number, client, status, address, created_at FROM parcel WHERE number = :number", sql.Named("number", number))
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return p, err
	}

	return p, err
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	var res []Parcel
	p := Parcel{}
	rows, err := s.db.Query("SELECT number, client, status, address, created_at FROM parcel WHERE client = :client", sql.Named("client", client))
	if err != nil {
		return res, err
	}
	err = rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return res, err
	}
	res = append(res, p)

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {

	_, err := s.db.Exec("UPDATE parcel SET status = :status WHERE number = :number",
		sql.Named("status", status),
		sql.Named("number", number))
	if err != nil {
		return err
	}
	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {

	var currentStatus string
	err := s.db.QueryRow("SELECT status FROM parcel WHERE number = :number", sql.Named("number", number)).Scan(&currentStatus)
	if err != nil {
		return err
	}

	if currentStatus != ParcelStatusRegistered {
		return fmt.Errorf("address can only be updated for parcels with status %s", ParcelStatusRegistered)
	}

	_, err = s.db.Exec("UPDATE parcel SET address = :address WHERE number = :number", sql.Named("address", address), sql.Named("number", number))
	if err != nil {
		return err
	}
	return nil
}

func (s ParcelStore) Delete(number int) error {

	var currentStatus string
	err := s.db.QueryRow("SELECT status FROM parcel WHERE number = :number", sql.Named("number", number)).Scan(&currentStatus)
	if err != nil {
		return err
	}

	if currentStatus != ParcelStatusRegistered {
		return fmt.Errorf("address can only be updated for parcels with status %s", ParcelStatusRegistered)
	}

	_, err = s.db.Exec("DELETE FROM parcel WHERE number = :number", sql.Named("number", number))
	if err != nil {
		return err
	}
	return nil
}
