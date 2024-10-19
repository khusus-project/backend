const express = require("express");
const cors = require("cors");
const fs = require("fs");
const path = require("path");
const axios = require("axios");
const csv = require("csv-parser");
const { PassThrough } = require("stream");
const bodyParser = require("body-parser");

const app = express();
app.use(cors());
app.use(express.json());
app.use(bodyParser.urlencoded({ extended: true }));

const PORT = 3001;
const CSV_URL =
  "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-GI0juM14m2HUcy2inBgyIDlk_Ebmo_4Wl6kN0kyiSRu4eV70GKafqFRH7duwlpvc4ygkx4hGqSQQ/pub?gid=1318934455&single=true&output=csv";
const CSV_FILE_PATH = path.join(__dirname, "/assets/dpt.csv");

let totalRows = 0; // Variabel global untuk menghitung total baris
let totalTPS = 0;

console.log(totalTPS);

// Fungsi untuk streaming data CSV dari URL
const streamCsvFromUrl = async () => {
  return new Promise((resolve, reject) => {
    totalRows = 0; // Reset counter
    const dataRows = [];

    axios({
      method: "get",
      url: CSV_URL,
      responseType: "stream",
    })
      .then((response) => {
        const csvStream = response.data.pipe(csv());
        csvStream
          .on("data", (row) => {
            totalRows++; // Menghitung jumlah baris
            dataRows.push(row);
          })
          .on("end", () => {
            resolve(dataRows); // Mengembalikan semua data setelah streaming selesai
          })
          .on("error", (error) => {
            reject(error);
          });
      })
      .catch((error) => {
        reject(error);
      });
  });
};

const csvDataSummary = async () => {
  return new Promise((resolve, reject) => {
    const dataRows = [];
    let totalDpt = 0;
    let uniqueDistrik = new Set();
    let uniqueKelurahan = new Set();
    let uniqueSlug = new Set();

    let ageCategories = {
      under17: { count: 0, percentage: 0, L: 0, P: 0 },
      b18sd25: { count: 0, percentage: 0, L: 0, P: 0 },
      b26sd35: { count: 0, percentage: 0, L: 0, P: 0 },
      b36sd55: { count: 0, percentage: 0, L: 0, P: 0 },
      b56sd60: { count: 0, percentage: 0, L: 0, P: 0 },
      over60: { count: 0, percentage: 0, L: 0, P: 0 },
    };

    axios({
      method: "get",
      url: CSV_URL,
      responseType: "stream",
    })
      .then((response) => {
        const csvStream = response.data.pipe(csv());
        csvStream
          .on("data", (row) => {
            const kecamatan = row["id_kecamatan"]; // Asumsi kolom Kecamatan
            const kelurahan = row["id_kelurahan"]; // Asumsi kolom Kelurahan
            const gender = row["gender"]; // Asumsi kolom Gender (L atau P)
            const age = parseInt(row["umur"]); // Asumsi kolom usia dinamakan 'usia'
            const slug = row["slug"]; // Asumsi kolom ID
            totalDpt++;

            if (slug) {
              uniqueSlug.add(slug);
            }

            if (kecamatan) {
              uniqueDistrik.add(kecamatan);
            }
            if (kelurahan) {
              uniqueKelurahan.add(kelurahan);
            }

            if (!isNaN(age)) {
              //   totalCount++; // Tambahkan total jumlah data

              // Kategorisasi usia
              if (age <= 17) {
                ageCategories.under17.count++;
                if (gender === "L") ageCategories.under17.L++;
                if (gender === "P") ageCategories.under17.P++;
              } else if (age >= 18 && age <= 25) {
                ageCategories["b18sd25"].count++;
                if (gender === "L") ageCategories["b18sd25"].L++;
                if (gender === "P") ageCategories["b18sd25"].P++;
              } else if (age >= 26 && age <= 35) {
                ageCategories["b26sd35"].count++;
                if (gender === "L") ageCategories["b26sd35"].L++;
                if (gender === "P") ageCategories["b26sd35"].P++;
              } else if (age >= 36 && age <= 55) {
                ageCategories["b36sd55"].count++;
                if (gender === "L") ageCategories["b36sd55"].L++;
                if (gender === "P") ageCategories["b36sd55"].P++;
              } else if (age >= 56 && age <= 60) {
                ageCategories["b56sd60"].count++;
                if (gender === "L") ageCategories["b56sd60"].L++;
                if (gender === "P") ageCategories["b56sd60"].P++;
              } else {
                ageCategories.over60.count++;
                if (gender === "L") ageCategories.over60.L++;
                if (gender === "P") ageCategories.over60.P++;
              }
            }
          })
          .on("end", () => {
            const jumlahDistrik = Array.from(uniqueDistrik).length;
            const jumlahKelurahan = Array.from(uniqueKelurahan).length;
            const jumlahTPS = Array.from(uniqueSlug).length;
            Object.keys(ageCategories).forEach((key) => {
              const totalInCategory = ageCategories[key].count;
              ageCategories[key].percentage = (
                (totalInCategory / totalDpt) *
                100
              ).toFixed(2); // Persentase
            });
            resolve({
              totalDpt,
              jumlahDistrik,
              jumlahKelurahan,
              jumlahTPS,
              ageCategories,
            });
          })
          .on("error", (error) => {
            reject(error); // Tangani error
          });
      })
      .catch((error) => {
        reject(error);
      });
  });
};

const csvTpsSummary = async () => {
  return new Promise((resolve, reject) => {
    const data = [];
    const summaryTps = [];

    const groupByTps = (row) => {
      const kelurahan = row.kelurahan;
      const kecamatan = row.kecamatan;
      const id_kelurahan = row.id_kelurahan;
      const id_kecamatan = row.id_kecamatan;
      const gender = row.gender;
      const slug = row.slug;
      const tps = row.tps;

      let existingTps = summaryTps.find((item) => item.slug === slug);

      if (!existingTps) {
        existingTps = {
          tps: tps,
          slug: slug,
          id_kecamatan: id_kecamatan,
          id_kelurahan: id_kelurahan,
          kecamatan: kecamatan,
          kelurahan: kelurahan,
          pria: 0,
          wanita: 0,
          totalDpt: 0,
        };
        summaryTps.push(existingTps);
      }

      if (gender === "L") {
        existingTps.pria += 1;
      } else if (gender === "P") {
        existingTps.wanita += 1;
      }
      existingTps.totalDpt++;
    };

    axios({
      method: "get",
      url: CSV_URL,
      responseType: "stream",
    })
      .then((response) => {
        const csvStream = response.data.pipe(csv());
        csvStream
          .on("data", (row) => {
            groupByTps(row);
          })
          .on("end", () => {
            resolve(summaryTps);
          })
          .on("error", (error) => {
            reject(error);
          });
      })
      .catch((error) => {
        reject(error);
      });
  });
};

const groupByKelurahan = (data) => {
  const summaryKelurahan = [];
  data.forEach((row) => {
    const id_kecamatan = row.id_kecamatan;
    const id_kelurahan = row.id_kelurahan;
    const kecamatan = row.kecamatan;
    const kelurahan = row.kelurahan;
    const pria = row.pria;
    const wanita = row.wanita;
    const totalDpt = row.totalDpt;

    let existingKelurahan = summaryKelurahan.find(
      (item) => item.id_kelurahan === id_kelurahan
    );

    if (!existingKelurahan) {
      existingKelurahan = {
        id_kecamatan: id_kecamatan,
        id_kelurahan: id_kelurahan,
        kecamatan: kecamatan,
        kelurahan: kelurahan,
        totalTps: 0,
        pria: 0,
        wanita: 0,
        totalDpt: 0,
      };
      summaryKelurahan.push(existingKelurahan);
    }
    existingKelurahan.pria += pria;
    existingKelurahan.wanita += wanita;
    existingKelurahan.totalDpt += totalDpt;
    existingKelurahan.totalTps += 1;
  });
  return summaryKelurahan;
};

app.get("/get-summary", async (req, res) => {
  try {
    const summary = await csvDataSummary(); // Streaming CSV dari fungsi csvDataSummary
    res.json(summary); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});
app.get("/get-summary-tps", async (req, res) => {
  try {
    const summary = await csvTpsSummary(); // Streaming CSV dari fungsi csvDataSummary
    res.json(summary); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

app.get("/get-summary-kelurahan", async (req, res) => {
  try {
    const data = await csvTpsSummary(); // Streaming CSV dari fungsi csvDataSummary
    const summary = await groupByKelurahan(data);
    res.json(summary); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

app.get("/get-all-data", async (req, res) => {
  try {
    const data = await streamCsvFromUrl(); // Streaming CSV dari fungsi csvDataSummary
    res.json(data); // Kirim hasil dalam format JSON
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

// Fungsi untuk streaming data CSV dan menghitung berdasarkan kecamatan dan kelurahan
const streamCsvAndCategorizeByDistrictAndVillage = async () => {
  return new Promise((resolve, reject) => {
    // Struktur data untuk hasil akhir
    let districtData = {};

    axios({
      method: "get",
      url: CSV_URL,
      responseType: "stream",
    })
      .then((response) => {
        const csvStream = response.data.pipe(csv());
        csvStream
          .on("data", (row) => {
            const kecamatan = row["id_kecamatan"]; // Asumsi kolom Kecamatan
            const kelurahan = row["kelurahan"]; // Asumsi kolom Kelurahan
            const gender = row["gender"]; // Asumsi kolom Gender (L atau P)

            if (!kecamatan || !kelurahan || !gender) {
              return; // Lewati data jika salah satu kolom tidak ada
            }

            // Jika kecamatan belum ada di objek districtData, inisialisasi
            if (!districtData[kecamatan]) {
              districtData[kecamatan] = {
                kelurahanCount: 0,
                kelurahanData: {},
              };
            }

            // Jika kelurahan belum ada di kecamatan tersebut, inisialisasi
            if (!districtData[kecamatan].kelurahanData[kelurahan]) {
              districtData[kecamatan].kelurahanData[kelurahan] = {
                total: 0,
                gender: {
                  L: 0, // Laki-laki
                  P: 0, // Perempuan
                },
              };
              // Tambahkan jumlah kelurahan pada kecamatan tersebut
              districtData[kecamatan].kelurahanCount++;
            }

            // Tambahkan total data pada kelurahan
            districtData[kecamatan].kelurahanData[kelurahan].total++;

            // Tambahkan jumlah berdasarkan gender
            if (gender === "L") {
              districtData[kecamatan].kelurahanData[kelurahan].gender.L++;
            } else if (gender === "P") {
              districtData[kecamatan].kelurahanData[kelurahan].gender.P++;
            }
          })
          .on("end", () => {
            resolve(districtData); // Selesaikan dan kirimkan hasil
          })
          .on("error", (error) => {
            reject(error); // Tangani error
          });
      })
      .catch((error) => {
        reject(error);
      });
  });
};

// Endpoint untuk pagination data CSV
app.get("/get-csv-data", async (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 10;
  const startIndex = (page - 1) * limit;
  const endIndex = page * limit;

  try {
    const data = await streamCsvFromUrl(); // Streaming data CSV dari URL
    const paginatedData = data.slice(startIndex, endIndex); // Ambil data sesuai halaman

    res.json({
      page,
      limit,
      data: paginatedData, // Kirim data dalam format JSON
    });
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

app.get("/district-village-gender", async (req, res) => {
  try {
    const result = await streamCsvAndCategorizeByDistrictAndVillage(); // Streaming CSV dan hitung kategori
    res.json(result); // Kirim hasil dalam format JSON
    console.log(result);
  } catch (error) {
    res.status(500).send("Error fetching CSV data");
  }
});

app.get("/", async (req, res) => {
  console.log("get from backend");
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
